import datetime
import decimal
import json
import logging
import time
import pprint
from collections import defaultdict
import boto3
import numpy as np
import pandas as pd
import requests
import tempfile
from configuration import config
from utils.debug import debug, dprint
from utils.retry import retry
from Client import Client
from utils import DynamoUtils, EmailUtils, S3Utils, LambdaUtils

BIDDING_LOOKBACK = 14  # days
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

# @debug
def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global clientsG
    global dynamodb
    global EMAIL_TO
    global logger
    
    EMAIL_TO = emailToInternal
    sendG = LambdaUtils.getSendG(env)
    dynamodb = LambdaUtils.getDynamoHost(env, dynamoEndpoint)
    clientsG = Client.getClients(dynamodb)

    logger = LambdaUtils.getLogger(env)
    logger.info("In runBranchBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


def return_active_keywords_dataFrame(ads_data, min_apple_installs, keyword_status, adgroup_deleted):
    # SK: This part is all business logic
    first_filter = ads_data[(ads_data["keywordStatus"] == "ACTIVE") & \
                            (ads_data["adGroupDeleted"] == "False")]

    group_by = first_filter.groupby(["adGroupId", "keywordId", "bid"]) \
        .agg({"installs": np.sum, \
              "branch_commerce_event_count": np.sum, \
              "branch_revenue": np.sum, \
              "localSpend": np.sum})
    second_filter = group_by[(group_by["installs"] >= min_apple_installs)].reset_index()
    return second_filter[
        ["adGroupId", "keywordId", "bid", "installs", "branch_commerce_event_count", "branch_revenue", "localSpend"]]

def return_cost_per_purchase_optimized_bid(active_keywords_dataFrame, branch_min_bid, branch_bid_adjustment,
                                           cost_per_purchase_threshold, cost_per_purchase_threshold_buffer):
    lower_cost_per_purchase_threshold = cost_per_purchase_threshold * (1 - cost_per_purchase_threshold_buffer)
    upper_cost_per_purchase_threshold = cost_per_purchase_threshold * (1 + cost_per_purchase_threshold_buffer)

    # JF this will throw an error if commerce event count is 0, handling this by iterating each row and caclulating cpp
    # active_keywords_dataFrame["cost_per_purchase"] = active_keywords_dataFrame["localSpend"] / \
    #                                                  active_keywords_dataFrame["branch_commerce_event_count"]
    cpp = []
    for row in active_keywords_dataFrame.itertuples():
        if (row.branch_commerce_event_count == 0) or (row.localSpend == 0):
            cpp.append(0)
        else:
            cpp.append(row.localSpend /
                       row.branch_commerce_event_count)

    active_keywords_dataFrame["cost_per_purchase"] = cpp
    active_keywords_dataFrame["adjusted_bid"] = active_keywords_dataFrame["bid"]

    active_keywords_dataFrame.loc[
        active_keywords_dataFrame["cost_per_purchase"] <= lower_cost_per_purchase_threshold, "adjusted_bid"] = \
        active_keywords_dataFrame["bid"] * (1 + branch_bid_adjustment)

    active_keywords_dataFrame.loc[(active_keywords_dataFrame["branch_commerce_event_count"] == 0) | (
            active_keywords_dataFrame["cost_per_purchase"] >= upper_cost_per_purchase_threshold), "adjusted_bid"] = \
        active_keywords_dataFrame["bid"] * (1 - branch_bid_adjustment)

    active_keywords_dataFrame.loc[
        active_keywords_dataFrame["adjusted_bid"] < branch_min_bid, "adjusted_bid"] = branch_min_bid
    return active_keywords_dataFrame[(active_keywords_dataFrame["adjusted_bid"] != active_keywords_dataFrame["bid"])]


def return_revenue_over_ad_spend_optimized_bid(active_keywords_dataFrame, branch_min_bid, branch_bid_adjustment,
                                               revenue_over_ad_spend_threshold, revenue_over_ad_spend_threshold_buffer):
    lower_revenue_over_ad_spend_threshold = revenue_over_ad_spend_threshold * (
            1 - revenue_over_ad_spend_threshold_buffer)
    upper_revenue_over_ad_spend_threshold = revenue_over_ad_spend_threshold * (
            1 + revenue_over_ad_spend_threshold_buffer)

    active_keywords_dataFrame["revenue_over_ad_spend"] = active_keywords_dataFrame["branch_revenue"] / \
                                                         active_keywords_dataFrame["localSpend"]

    active_keywords_dataFrame["adjusted_bid"] = active_keywords_dataFrame["bid"]

    active_keywords_dataFrame.loc[
        active_keywords_dataFrame["revenue_over_ad_spend"] <= lower_revenue_over_ad_spend_threshold, "adjusted_bid"] = \
        active_keywords_dataFrame["bid"] * (1 - branch_bid_adjustment)

    active_keywords_dataFrame.loc[
        active_keywords_dataFrame["revenue_over_ad_spend"] >= upper_revenue_over_ad_spend_threshold, "adjusted_bid"] = \
        active_keywords_dataFrame["bid"] * (1 + branch_bid_adjustment)

    active_keywords_dataFrame.loc[
        active_keywords_dataFrame["adjusted_bid"] < branch_min_bid, "adjusted_bid"] = branch_min_bid
    return active_keywords_dataFrame[(active_keywords_dataFrame["adjusted_bid"] != active_keywords_dataFrame["bid"])]


def return_adjusted_bids(branch_optimization_goal, active_keywords_dataFrame, branch_min_bid, branch_bid_adjustment,
                         cost_per_purchase_threshold, cost_per_purchase_threshold_buffer,
                         revenue_over_ad_spend_threshold, revenue_over_ad_spend_threshold_buffer):
    if branch_optimization_goal == "cost_per_purchase":
        print("Optimizing Cost Per Purchase")
        return return_cost_per_purchase_optimized_bid(active_keywords_dataFrame, branch_min_bid, branch_bid_adjustment,
                                                      cost_per_purchase_threshold, cost_per_purchase_threshold_buffer)
    elif branch_optimization_goal == "revenue_over_ad_spend":
        print("Optimizing Revenue Over Ad Spend")
        return return_revenue_over_ad_spend_optimized_bid(active_keywords_dataFrame, branch_min_bid,
                                                          branch_bid_adjustment, revenue_over_ad_spend_threshold,
                                                          revenue_over_ad_spend_threshold_buffer)
    else:
        print("Unknown Optimization Goal")
        # TODO JF why are we returning here
        return pd.DataFrame(columns=['id'])


def create_json_from_dataFrame(filtered_dataframe):
    output_adGroupId_dict = {}
    unique_adGroupId_array = filtered_dataframe.adGroupId.unique()

    for adGroupId in unique_adGroupId_array:
        output_adGroupId_dict[adGroupId] = []
        for ind in filtered_dataframe[filtered_dataframe["adGroupId"] == adGroupId].index:
            output_adGroupId_dict[adGroupId].append({ \
                "id": int(filtered_dataframe["keywordId"][ind]), \
                "bidAmount": { \
                    "amount": str(round(filtered_dataframe["adjusted_bid"][ind], 2)), \
                    "currency": "USD" \
                    } \
                })
    return output_adGroupId_dict

# TODO rework this to use config
def create_put_request_string(campaign_id, adgroup_id):
    return "https://api.searchads.apple.com/api/v3/campaigns/{}/adgroups/{}/targetingkeywords/bulk".format(
        campaign_id, adgroup_id)

def write_request_file(put_request_string, request_json, request_output_filename):
    try:
        out_file_object = open(request_output_filename, "w")
    except:
        print("Error: Issues opening the output file")
    out_file_object.write(put_request_string)
    out_file_object.write("\n\n")
    out_file_object.write(str(request_json))
    out_file_object.close()

# @debug
def process():
    summaryReportInfo = {}

    for client in clientsG:
        summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = clientSummaryReportInfo = {}
        keyword_status = "ACTIVE"
        adgroup_deleted = "False"

        try:
            branch_key = client.branchIntegrationParameters["branch_key"]
            branch_secret = client.branchIntegrationParameters["branch_secret"]
            run_branch = True
        except KeyError as error:
            logger.info("runBranchIntegration.process:::no branch config skipping!" + str(client.orgId))
            run_branch = False

        if not run_branch:
            continue

        adgroup_keys = client.keywordAdderIds["adGroupId"].keys()
        for adgroup_key in adgroup_keys:
            adgroup_id = client.keywordAdderIds["adGroupId"][adgroup_key]
            campaign_id = str(client.keywordAdderIds["campaignId"][adgroup_key])
            print("pulling adgroup_id " + str(adgroup_id))
            print("campaign id " + str(campaign_id))
                
            # get apple data
            kw_response = DynamoUtils.getAppleKeywordData(dynamodb, adgroup_id, start_date, end_date)
            print("querying with:::" + str(start_date) + " - " + str(end_date))
            print("got back:::" + str(kw_response["Count"]))

            if (kw_response["Count"] == 0):
                print("skipping")
                continue

            keyword_info = defaultdict(list)
            for kw_data in kw_response[u'Items']:
                keyword = kw_data['keyword']
                date = kw_data['date']
                branch_revenue = 0
                branch_commerce_event_count = 0

                # get branch data
                branch_response = DynamoUtils.getBranchCommerceEvents(dynamodb, campaign_id, adgroup_id, keyword, date)
                for j in branch_response[u'Items']:
                    print("found branch result!")
                    print(json.dumps(j, cls=DecimalEncoder))
                    if len(branch_response['Items']) > 0:
                        branch_revenue = int(branch_response['Items'][0]["revenue"])
                        branch_commerce_event_count = int(branch_response['Items'][0]["count"])

                # initialize data frame
                keyword_info["keyword"].append(kw_data["keyword"])
                keyword_info["keywordId"].append(kw_data["keyword_id"])
                keyword_info["keywordStatus"].append(kw_data["keywordStatus"])
                keyword_info["matchType"].append(kw_data["matchType"])
                keyword_info["adGroupName"].append(kw_data["adgroup_name"])
                keyword_info["adGroupId"].append(kw_data["adgroup_id"])
                keyword_info["adGroupDeleted"].append(kw_data["adgroup_deleted"])
                keyword_info["bid"].append(kw_data["bid"])
                keyword_info["deleted"].append(kw_data["deleted"])
                keyword_info["keywordDisplayStatus"].append(kw_data["keywordDisplayStatus"])
                keyword_info["modificationTime"].append(kw_data["modification_time"])
                keyword_info["date"].append(kw_data["date"])
                keyword_info["impressions"].append(kw_data["impressions"])
                keyword_info["taps"].append(kw_data["taps"])
                keyword_info["ttr"].append(kw_data["ttr"])
                keyword_info["installs"].append(kw_data["installs"])
                keyword_info["newDownloads"].append(kw_data["new_downloads"])
                keyword_info["redownloads"].append(kw_data["re_downloads"])
                keyword_info["latOnInstalls"].append(kw_data["lat_on_installs"])
                keyword_info["latOffInstalls"].append(kw_data["lat_off_installs"])
                keyword_info["avgCPA"].append(kw_data["avg_cpa"])
                keyword_info["conversionRate"].append(kw_data["conversion_rate"])
                keyword_info["localSpend"].append(kw_data["local_spend"])
                keyword_info["avgCPT"].append(kw_data["avg_cpt"])

                # branch fields
                keyword_info["branch_commerce_event_count"].append(branch_commerce_event_count)
                keyword_info["branch_revenue"].append(branch_revenue)

            raw_data_df = pd.DataFrame(keyword_info)
            BBP = client.branchBidParameters
            min_apple_installs = BBP["min_apple_installs"]

            # send email
            #fp = tempfile.NamedTemporaryFile(dir="/tmp", delete=False)
            fp = tempfile.NamedTemporaryFile(dir=".", delete=False)
            raw_data_df.to_csv(fp.name)
            # EmailUtils.sendRawEmail("test", "runBrachBidAdjuster Debugging", EMAIL_TO, [], config.EMAIL_FROM, fp.name)
            
            
            if raw_data_df.empty:
                print("Error: There was an issue reading the data to a dataFrame")
                continue

            active_keywords = return_active_keywords_dataFrame(
                raw_data_df, 
                min_apple_installs,
                keyword_status,
                adgroup_deleted
            )
                    
            if active_keywords.empty:
                print("There weren't any keywords that met the initial filtering criteria")
                continue

            print("There were keywords that met the initial filtering criteria")
            branch_optimization_goal = BBP["branch_optimization_goal"]
            branch_min_bid = BBP["branch_min_bid"]
            branch_bid_adjustment = decimal.Decimal.from_float(float(BBP["branch_bid_adjustment"]))
            cost_per_purchase_threshold = BBP["cost_per_purchase_threshold"]
            cost_per_purchase_threshold_buffer = BBP["cost_per_purchase_threshold_buffer"]
            revenue_over_ad_spend_threshold = BBP["revenue_over_ad_spend_threshold"]
            revenue_over_ad_spend_threshold_buffer = BBP["revenue_over_ad_spend_threshold_buffer"]

            adjusted_bids = return_adjusted_bids(
                branch_optimization_goal,
                active_keywords,
                branch_min_bid,
                branch_bid_adjustment,
                cost_per_purchase_threshold,
                cost_per_purchase_threshold_buffer,
                revenue_over_ad_spend_threshold,
                revenue_over_ad_spend_threshold_buffer
            )

            if adjusted_bids.empty:
                print("There weren't any bids to adjust")
                continue
                    
            json_data = create_json_from_dataFrame(adjusted_bids)
            clientSummaryReportInfo[campaign_id] = json.dumps(json_data)
            for adGroupId in json_data.keys():
                put_request_string = create_put_request_string(campaign_id, str(adGroupId))
                request_json = json_data[adGroupId]
                sendUpdatedBidsToApple(
                    client, 
                    put_request_string, 
                    request_json
                )

    emailSummaryReport(summaryReportInfo, sendG)

@retry
def sendUpdatedBidsToAppleHelper(url, cert, json, headers):
    return requests.put(
        url, 
        cert=cert, 
        json=json, 
        headers=headers, 
        timeout=config.HTTP_REQUEST_TIMEOUT
    )


def sendUpdatedBidsToApple(client, url, payload):
    headers = {
        "Authorization": "orgId=%s" % client.orgId,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    dprint("\nURL is '%s'." % url)
    dprint("\nPayload is '%s'." % payload)
    dprint("\nHeaders are %s." % headers)

    if url and payload:
        if sendG:
            response = sendUpdatedBidsToAppleHelper(
                url,
                cert=(S3Utils.getCert(client.pemFilename), S3Utils.getCert(client.keyFilename)),
                json=payload,
                headers=headers
            )
        else:
            response = "Not actually sending anything to Apple."
        print("The result of sending the update to Apple: %s" % response)
    return sendG


# @debug
def createEmailBody(data, sent):
  content = ["""Sent to Apple is %s.""" % sent,
             """\t""".join(["Client", "Campaign", "Updated Branch Bids"])]

  for client, clientData in data.items():
    content.append(client)
    for campaignId, payload in clientData.items():
        #content.append("""\t%s\t%s""" % (campaignId, payload))
        content.append("""\t%s\t%s""" % (campaignId,  pprint.pformat(payload)))
     

  return "\n".join(content)


# @debug
def emailSummaryReport(data, sent):
    messageString = createEmailBody(data, sent)
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = "Branch Bid Adjuster summary for %s" % dateString
    EmailUtils.sendTextEmail(messageString, subjectString, EMAIL_TO, [], config.EMAIL_FROM)


# @debug
def terminate():
    pass


if __name__ == "__main__":
    initialize('lcl', 'http://localhost:8000', ["james@adoya.io"])
    process()
    terminate()

def lambda_handler(event, context):
    initialize(event['env'], event['dynamoEndpoint'], event['emailToInternal'])
    process()
    terminate()
    return {
        'statusCode': 200,
        'body': json.dumps('Run Branch Bid Adjuster Complete')
    }