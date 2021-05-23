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
from utils.DecimalEncoder import DecimalEncoder
import sys

BIDDING_LOOKBACK = 14  # days
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta


def initialize(clientEvent):
    global sendG
    global clientG
    global emailToG
    global dynamodb
    global logger
    global authToken
    
    emailToG = clientEvent['rootEvent']['emailToInternal']
    sendG = LambdaUtils.getSendG(
        clientEvent['rootEvent']['env']
    )
    dynamodb = LambdaUtils.getDynamoResource(
        clientEvent['rootEvent']['env'],
        clientEvent['rootEvent']['dynamoEndpoint']
    ) 
    clientG = Client.buildFromDictionary(
        json.loads(
            clientEvent['orgDetails']
        )
    )
    authToken = clientEvent['authToken']
    logger = LambdaUtils.getLogger(
        clientEvent['rootEvent']['env']
    )
    logger.info(
         "runBranchBidAdjuster:::initialize(), rootEvent='" + str(clientEvent['rootEvent'])
    )


def returnActiveKeywordsDataFrame(
    ads_data, 
    min_apple_installs, 
    keyword_status, 
    adgroup_deleted
):
    first_filter = ads_data[
        (ads_data["keywordStatus"] == "ACTIVE") & (ads_data["adGroupDeleted"] == "False")
    ]

    latest_row = first_filter \
        .sort_values('date') \
        .groupby('keywordId') \
        .tail(1) \
        .reset_index(drop=True)

    latest_bid = latest_row \
        .filter(items=["bid", "keywordId"]) \
        .sort_values("keywordId") \
        .reset_index(drop=True)

    aggregated = first_filter \
        .groupby(["adGroupId", "keywordId"]) \
        .agg(
            {
                "installs": np.sum,
                "branch_commerce_event_count": np.sum,
                "branch_revenue": np.sum,
                "localSpend": np.sum
            }
        ) \
        .sort_values("keywordId") \
        .reset_index(drop=False)

    concatenated = aggregated.merge(latest_bid, left_on="keywordId", right_on="keywordId")
    second_filter = concatenated[(concatenated["installs"] >= min_apple_installs)].reset_index()
    return second_filter[
        [
            "adGroupId", 
            "keywordId", 
            "bid", 
            "installs", 
            "branch_commerce_event_count", 
            "branch_revenue", 
            "localSpend"
        ]
    ]

def returnCostPerPurchaseOptimizedBid(
    active_keywords_dataFrame, 
    branch_min_bid,
    branch_max_bid, 
    branch_bid_adjustment,
    cost_per_purchase_threshold, 
    cost_per_purchase_threshold_buffer
):
    lower_cost_per_purchase_threshold = cost_per_purchase_threshold * (1 - cost_per_purchase_threshold_buffer)
    upper_cost_per_purchase_threshold = cost_per_purchase_threshold * (1 + cost_per_purchase_threshold_buffer)

    # NOTE this will throw an error if commerce event count is 0, handling this by iterating each row and caclulating cpp
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

    active_keywords_dataFrame.loc[active_keywords_dataFrame["adjusted_bid"] < branch_min_bid, "adjusted_bid"] = branch_min_bid
    active_keywords_dataFrame.loc[active_keywords_dataFrame["adjusted_bid"] > branch_max_bid, "adjusted_bid"] = branch_max_bid

    
    return active_keywords_dataFrame[(active_keywords_dataFrame["adjusted_bid"] != active_keywords_dataFrame["bid"])]


def returnRevenueOverAdSpendOptimized(
    active_keywords_dataFrame, 
    branch_min_bid,
    branch_max_bid, 
    branch_bid_adjustment,
    revenue_over_ad_spend_threshold, 
    revenue_over_ad_spend_threshold_buffer
):
    lower_revenue_over_ad_spend_threshold = revenue_over_ad_spend_threshold * (
            1 - revenue_over_ad_spend_threshold_buffer)
    upper_revenue_over_ad_spend_threshold = revenue_over_ad_spend_threshold * (
            1 + revenue_over_ad_spend_threshold_buffer)

    # NOTE this will throw an error if commerce event count is 0, handling this by iterating each row and caclulating cpp
    # active_keywords_dataFrame["revenue_over_ad_spend"] = active_keywords_dataFrame["branch_revenue"] / \
    #                                                      active_keywords_dataFrame["localSpend"]
    roas = []
    for row in active_keywords_dataFrame.itertuples():
        if (row.branch_revenue == 0) or (row.localSpend == 0):
            roas.append(0)
        else:
            roas.append(row.branch_revenue /
                       row.localSpend)

    active_keywords_dataFrame["revenue_over_ad_spend"] = roas

    active_keywords_dataFrame["adjusted_bid"] = active_keywords_dataFrame["bid"]

    active_keywords_dataFrame.loc[
        active_keywords_dataFrame["revenue_over_ad_spend"] <= lower_revenue_over_ad_spend_threshold, "adjusted_bid"] = \
        active_keywords_dataFrame["bid"] * (1 - branch_bid_adjustment)

    active_keywords_dataFrame.loc[
        active_keywords_dataFrame["revenue_over_ad_spend"] >= upper_revenue_over_ad_spend_threshold, "adjusted_bid"] = \
        active_keywords_dataFrame["bid"] * (1 + branch_bid_adjustment)

    active_keywords_dataFrame.loc[active_keywords_dataFrame["adjusted_bid"] < branch_min_bid, "adjusted_bid"] = branch_min_bid
    active_keywords_dataFrame.loc[active_keywords_dataFrame["adjusted_bid"] > branch_max_bid, "adjusted_bid"] = branch_max_bid

    return active_keywords_dataFrame[(active_keywords_dataFrame["adjusted_bid"] != active_keywords_dataFrame["bid"])]


def returnAdjustedBids(
    branch_optimization_goal, 
    active_keywords_dataFrame, 
    branch_min_bid,
    branch_max_bid, 
    branch_bid_adjustment,
    cost_per_purchase_threshold, 
    cost_per_purchase_threshold_buffer,
    revenue_over_ad_spend_threshold, 
    revenue_over_ad_spend_threshold_buffer
):
    if branch_optimization_goal == "cost_per_purchase":
        print("Optimizing Cost Per Purchase")
        return returnCostPerPurchaseOptimizedBid(
            active_keywords_dataFrame, 
            branch_min_bid,
            branch_max_bid, 
            branch_bid_adjustment,
            cost_per_purchase_threshold, 
            cost_per_purchase_threshold_buffer
        )
    elif branch_optimization_goal == "revenue_over_ad_spend":
        print("Optimizing Revenue Over Ad Spend")
        return returnRevenueOverAdSpendOptimized(
            active_keywords_dataFrame, 
            branch_min_bid,
            branch_max_bid,
            branch_bid_adjustment, 
            revenue_over_ad_spend_threshold,
            revenue_over_ad_spend_threshold_buffer
        )
    else:
        print("Unknown Optimization Goal")
        return False


def createJsonFromDataFrame(filtered_dataframe):
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
def createPutRequestString(campaignId, adgroupId):
    return "https://api.searchads.apple.com/api/v4/campaigns/{}/adgroups/{}/targetingkeywords/bulk".format(
        campaignId, adgroupId)


def createDataFrame(items, campaign_id, adgroup_id):
    keyword_info = defaultdict(list)
    for kw_data in items:
        keyword = kw_data['keyword']
        date = kw_data['date']
        branch_revenue = 0
        branch_commerce_event_count = 0
       
        # NOTE incrementing on each loop thru items is unneeded as we query with timestamp, and branch data is by day at its lowest level
        branch_response = DynamoUtils.getBranchCommerceEvents(dynamodb, campaign_id, adgroup_id, keyword, date)
        for j in branch_response.get("Items"):
            print("found branch result!")
            # print(json.dumps(j, cls=DecimalEncoder))
            branch_revenue = branch_revenue + int(j.get("revenue",0))
            branch_commerce_event_count = branch_commerce_event_count + int(j.get("count",0))
        
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
        keyword_info["branch_commerce_event_count"].append(branch_commerce_event_count) #branch field
        keyword_info["branch_revenue"].append(branch_revenue) #branch field

    return pd.DataFrame(keyword_info)

def process():
    summaryReportInfo = {}

    summaryReportInfo["%s (%s)" % (clientG.orgId, clientG.clientName)] = clientSummaryReportInfo = {}
    keywordStatus = "ACTIVE"
    adgroupDeleted = "False"
    if not clientG.branchIntegrationParameters.get("branch_bid_adjuster_enabled",False):
        return

    appleCampaigns = clientG.appleCampaigns
    campaignsForBidAdjuster = list(
        filter(
            lambda campaign:(campaign.get("branchBidAdjusterEnabled",False) == True), appleCampaigns
        )
    )
    for campaign in campaignsForBidAdjuster:
        BBP = LambdaUtils.getBidParamsForJob(clientG.__dict__, campaign, "branchBidAdjuster")
        print("bidParameters" + str(BBP))
        min_apple_installs = BBP["min_apple_installs"]
        branch_optimization_goal = BBP["branch_optimization_goal"]
        branch_min_bid = BBP["branch_min_bid"]
        branch_max_bid = BBP["branch_max_bid"]
        branch_bid_adjustment = decimal.Decimal.from_float(float(BBP["branch_bid_adjustment"]))
        cost_per_purchase_threshold_buffer = BBP["cost_per_purchase_threshold_buffer"]
        revenue_over_ad_spend_threshold_buffer = BBP["revenue_over_ad_spend_threshold_buffer"]
        cost_per_purchase_threshold = BBP['cost_per_purchase_threshold']
        revenue_over_ad_spend_threshold = BBP['revenue_over_ad_spend_threshold']
        # get apple data
        kwResponse = DynamoUtils.getAppleKeywordData(dynamodb, campaign['adGroupId'], start_date, end_date)
        print("querying" + campaign['adGroupId'] +  ":::with:::" + str(start_date) + " - " + str(end_date))
        print("got back:::" + str(kwResponse["Count"]))
        if (kwResponse["Count"] == 0):
            print("skipping")
            continue
        # build dataframe
        rawDataDf = createDataFrame(kwResponse.get('Items'), campaign['campaignId'], campaign['adGroupId'])
        # fp = tempfile.NamedTemporaryFile(dir="/tmp", delete=False)
        # fp = tempfile.NamedTemporaryFile(dir=".", delete=False)
        # rawDataDf.to_csv(campaign['adGroupId'] + 'rawDataDf' + ".csv")
        # EmailUtils.sendRawEmail("test", "runBrachBidAdjuster Debugging", emailToG, [], config.EMAIL_FROM, fp.name)
           
        if rawDataDf.empty:
            print("Error: There was an issue reading the data to a dataFrame")
            continue
        activeKeywords = returnActiveKeywordsDataFrame(
            rawDataDf, 
            min_apple_installs,
            keywordStatus,
            adgroupDeleted
        )
            
        if activeKeywords.empty:
            print("There weren't any keywords that met the initial filtering criteria")
            continue
        print("There were keywords that met the initial filtering criteria")
        adjustedBids = returnAdjustedBids(
            branch_optimization_goal,
            activeKeywords,
            branch_min_bid,
            branch_max_bid,
            branch_bid_adjustment,
            cost_per_purchase_threshold,
            cost_per_purchase_threshold_buffer,
            revenue_over_ad_spend_threshold,
            revenue_over_ad_spend_threshold_buffer
        )
        if adjustedBids.empty:
            print("There weren't any bids to adjust")
            continue
                
        jsonData = createJsonFromDataFrame(adjustedBids)
        clientSummaryReportInfo[campaign['campaignId']] = json.dumps(jsonData)
        for adGroupId in jsonData.keys():
            putRequestString = createPutRequestString(campaign['campaignId'], campaign['adGroupId'])
            requestJson = jsonData[adGroupId]
            sendUpdatedBidsToApple(
                putRequestString, 
                requestJson
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

@retry
def sendUpdatedBidsByTokenHelper(url, json, headers):
    return requests.put(
        url, 
        json=json, 
        headers=headers, 
        timeout=config.HTTP_REQUEST_TIMEOUT
    )


def sendUpdatedBidsToApple(url, payload):
    if authToken is not None:
        headers = {
            "Authorization": "Bearer %s" % authToken, 
            "X-AP-Context": "orgId=%s" % clientG.orgId,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    else:
        headers = {
            "Authorization": "orgId=%s" % clientG.orgId,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    dprint("\nURL is '%s'." % url)
    dprint("\nPayload is '%s'." % payload)
    dprint("\nHeaders are %s." % headers)

    if url and payload:
        if sendG:
            # NOTE pivot on token unt
            if authToken is not None:
                response = sendUpdatedBidsByTokenHelper(
                url,
                json=payload,
                headers=headers
            )
            else:
                response = sendUpdatedBidsToAppleHelper(
                    url,
                    cert=(S3Utils.getCert(clientG.pemFilename), S3Utils.getCert(clientG.keyFilename)),
                    json=payload,
                    headers=headers
                )
        else:
            response = "Not actually sending anything to Apple."
        print("The result of sending the update to Apple: %s" % response)
    return sendG


def createEmailBody(data, sent):
  content = ["""Sent to Apple is %s.""" % sent,
             """\t""".join(["Client", "Campaign", "Updated Branch Bids"])]

  for client, clientData in data.items():
    content.append(client)
    for campaignId, payload in clientData.items():
        content.append("""\t%s\t%s""" % (campaignId,  pprint.pformat(payload)))
     
  return "\n".join(content)


def emailSummaryReport(data, sent):
    messageString = createEmailBody(data, sent)
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = "Branch Bid Adjuster summary for %s" % dateString
    EmailUtils.sendTextEmail(messageString, subjectString, emailToG, [], config.EMAIL_FROM)


if __name__ == "__main__":
    clientEvent = LambdaUtils.getClientForLocalRun(
        int(sys.argv[1]),
        ['james@adoya.io']
    )
    initialize(clientEvent)
    process()

def lambda_handler(clientEvent):
    initialize(clientEvent)
    try:
        process()
    except: 
        return {
            'statusCode': 400,
            'body': json.dumps('Run Branch Bid Adjuster Failed')
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Branch Bid Adjuster Complete for ' + clientG.clientName)
    }