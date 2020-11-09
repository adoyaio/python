import decimal
import datetime
import json
import numpy as np
import pandas as pd
import pprint
import requests
import time
import boto3
import logging
from collections import defaultdict
from datetime import datetime as dt
from boto3.dynamodb.conditions import Key, Attr
from utils.debug import debug, dprint
from utils.retry import retry
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
from Client import Client
from configuration import config

BIDDING_LOOKBACK = 14
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta

# cpi history kookback seperate from apple lookback
start_date_delta_cpi_lookback = datetime.timedelta(config.TOTAL_COST_PER_INSTALL_LOOKBACK)
start_date_cpi_lookback = today - start_date_delta_cpi_lookback

# for qa set fields explicitly
# start_date = dt.strptime('2019-12-01', '%Y-%m-%d').date()
# end_date = dt.strptime('2019-12-08', '%Y-%m-%d').date()

@debug
def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global clientsG
    global dynamodb
    global EMAIL_TO
    global logger
    
    EMAIL_TO = emailToInternal
    sendG = LambdaUtils.getSendG(env)
    dynamodb = LambdaUtils.getDynamoHost(env,dynamoEndpoint)
    clientsG = Client.getClients(dynamodb)
    logger = LambdaUtils.getLogger(env)
    logger.info("runBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s', emailTo='%s'" % (
        sendG, dynamoEndpoint, str(EMAIL_TO)))


@retry
def getKeywordReportFromAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

def getKeywordReportFromApple(client, campaignId):
    payload = {
        "startTime": str(start_date),
        "endTime": str(end_date),
        # "granularity": 2, # 1=hourly, 2=daily, 3=monthly, etc.
        "selector": {
            "orderBy": [
                {
                    "field": "localSpend",
                    "sortOrder": "DESCENDING"
                }
            ],
            "fields": [
                "localSpend",
                "taps",
                "impressions",
                "installs",
                "avgCPA",
                "avgCPT",
                "ttr",
                "conversionRate"
            ],
            "pagination": {
                "offset": 0,
                "limit": 1000
            }
        },
        # "groupBy" : ["COUNTRY_CODE"],
        "returnRowTotals": True,
        "returnRecordsWithNoMetrics": True
    }
    url = config.APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaignId
    headers = {"Authorization": "orgId=%s" % client.orgId}
    dprint("\nURL is %s" % url)
    dprint("\nPayload is %s" % payload)
    dprint("\nHeaders are %s" % headers)
    response = getKeywordReportFromAppleHelper(
        url,
        cert=(S3Utils.getCert(client.pemFilename),S3Utils.getCert(client.keyFilename)),
        json=payload,
        headers=headers
    )
    dprint("Response is %s" % response)

    if response.status_code == 200:
        return json.loads(response.text)
    else:
        email = "client id:%d \n url:%s \n response:%s" % (client.orgId, url, response)
        date = time.strftime("%m/%d/%Y")
        subject ="%s - %d ERROR in runBidAdjuster for %s" % (date, response.status_code, client.clientName)
        logger.warn(email)
        logger.error(subject)
        if sendG:
            EmailUtils.sendTextEmail(email, subject, EMAIL_TO, [], config.EMAIL_FROM)
        return False
   

def createUpdatedKeywordBids(data, campaignId, client):
    rows = data["data"]["reportingDataResponse"]["row"]
    
    if len(rows) == 0:
        return False

    keyword_info = defaultdict(list)
    summaryReportInfo = {}
    for row in rows:
        metadata = row["metadata"]
        summaryReportInfo[metadata["keywordId"]] = {
            "keyword": metadata["keyword"],
            "oldBid": metadata["bidAmount"]["amount"],
        }
        keyword_info["keyword"].append(metadata["keyword"])
        keyword_info["keywordId"].append(metadata["keywordId"])
        keyword_info["keywordStatus"].append(metadata["keywordStatus"])
        keyword_info["matchType"].append(metadata["matchType"])
        keyword_info["adGroupName"].append(metadata["adGroupName"])
        keyword_info["adGroupId"].append(metadata["adGroupId"])
        keyword_info["adGroupDeleted"].append(metadata["adGroupDeleted"])
        keyword_info["bid"].append(metadata["bidAmount"]["amount"])
        keyword_info["deleted"].append(metadata["deleted"])
        keyword_info["keywordDisplayStatus"].append(metadata["keywordDisplayStatus"])
        keyword_info["modificationTime"].append(metadata["modificationTime"])
        totals = row["total"]
        keyword_info["impressions"].append(totals["impressions"])
        keyword_info["taps"].append(totals["taps"])
        keyword_info["ttr"].append(totals["ttr"])
        keyword_info["installs"].append(totals["installs"])
        keyword_info["newDownloads"].append(totals["newDownloads"])
        keyword_info["redownloads"].append(totals["redownloads"])
        keyword_info["latOnInstalls"].append(totals["latOnInstalls"])
        keyword_info["latOffInstalls"].append(totals["latOffInstalls"])
        keyword_info["avgCPA"].append(totals["avgCPA"]["amount"])
        keyword_info["conversionRate"].append(totals["conversionRate"])
        keyword_info["localSpend"].append(totals["localSpend"]["amount"])
        keyword_info["avgCPT"].append(totals["avgCPT"]["amount"])

    # JF this extremely verbose don't use in PROD
    # dprint("keyword_info=%s." % pprint.pformat(keyword_info))
    # convert to dataframe
    df_keyword_info = pd.DataFrame(keyword_info)
    dprint("df_keyword_info=%s." % str(df_keyword_info))

    # pull in active keywords only
    # Changed, 15-Sep-18 per email from Scott Kaplan of 8-Sep-18
    #  keyword_info = keyword_info[keyword_info['keywordStatus'] == 'ACTIVE']
    df_keyword_info = df_keyword_info[df_keyword_info['keywordDisplayStatus'] == 'RUNNING']

    # extract only the columns you need for keyword bids
    ex_keyword_info = df_keyword_info[
        [
            "keyword",
            "matchType",
            "adGroupId",
            "keywordId",
            "impressions",
            "taps",
            "installs",
            "avgCPA",
            "localSpend",
            "bid"
        ]
    ]

    dprint("ex_keyword_info=%s." % str(ex_keyword_info))
    BP = client.bidParameters

    # first convert avg cpa to float so you can perform calculations
    ex_keyword_info["avgCPA"] = ex_keyword_info["avgCPA"].astype(float)
    ex_keyword_info["bid"] = ex_keyword_info["bid"].astype(float)

    # calculate bid multiplier and create a new column
    ex_keyword_info["bid_multiplier"] = BP["HIGH_CPI_BID_DECREASE_THRESH"] / ex_keyword_info["avgCPA"]

    # cap bid multiplier
    if BP["OBJECTIVE"] == "aggressive":
        ex_keyword_info['bid_multiplier_capped'] = np.clip(ex_keyword_info['bid_multiplier'], 0.90, 1.30)
    elif BP["OBJECTIVE"] == "standard":
        ex_keyword_info['bid_multiplier_capped'] = np.clip(ex_keyword_info['bid_multiplier'], 0.80, 1.20)
    elif BP["OBJECTIVE"] == "conservative":
        ex_keyword_info['bid_multiplier_capped'] = np.clip(ex_keyword_info['bid_multiplier'], 0.70, 1.10)
    else:
        print("no objective selected default to standard")
        ex_keyword_info['bid_multiplier_capped'] = np.clip(ex_keyword_info['bid_multiplier'], 0.80, 1.20)

    # create upper bid cap tied to target cost per install.
    if BP["OBJECTIVE"] == "aggressive":
        bidCap_targetCPI = BP["HIGH_CPI_BID_DECREASE_THRESH"] * 1.20
    elif BP["OBJECTIVE"] == "standard":
        bidCap_targetCPI = BP["HIGH_CPI_BID_DECREASE_THRESH"] * 1
    elif BP["OBJECTIVE"] == "conservative":
        bidCap_targetCPI = BP["HIGH_CPI_BID_DECREASE_THRESH"] * 0.80
    else:
        print("no objective selected default to standard")
        bidCap_targetCPI = BP["HIGH_CPI_BID_DECREASE_THRESH"] * 1

    # subset keywords for stale raises
    stale_raise_kws = ex_keyword_info[ex_keyword_info["taps"] < BP["TAP_THRESHOLD"]]

    # subset keywords for bid increase
    low_cpa_keywords = ex_keyword_info[
        (ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
        (ex_keyword_info["avgCPA"] <= BP["HIGH_CPI_BID_DECREASE_THRESH"]) & \
        (ex_keyword_info["installs"] > BP["NO_INSTALL_BID_DECREASE_THRESH"])
    ]

    # subset keywords for bid decrease
    high_cpa_keywords = ex_keyword_info[
        (ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
        (ex_keyword_info["avgCPA"] > BP["HIGH_CPI_BID_DECREASE_THRESH"]) & \
        (ex_keyword_info["installs"] > BP["NO_INSTALL_BID_DECREASE_THRESH"])
    ]

    no_install_keywords = ex_keyword_info[
        (ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
        (ex_keyword_info["installs"] == BP["NO_INSTALL_BID_DECREASE_THRESH"])
    ]

    # raise bids for stale raise keywords
    stale_raise_kws["new_bid"] = (stale_raise_kws["bid"] * BP["STALE_RAISE_BID_BOOST"]).round(2)

    # raise bids for low cpi keywords using new bid multiplier cap
    low_cpa_keywords["new_bid"] = (low_cpa_keywords["bid"] * low_cpa_keywords["bid_multiplier_capped"]).round(2)

    # check if overall CPI is within bid threshold, if not, fix it. JF 05/31/2020 use CPI Lookback vs apple lookback
    total_cost_per_install = client.getTotalCostPerInstall(
        dynamodb, 
        start_date_cpi_lookback, 
        end_date,
        config.TOTAL_COST_PER_INSTALL_LOOKBACK
    )
    dprint("total cpi %s" % str(total_cost_per_install))

    # if cpi is below threshold, only do increases
    if total_cost_per_install > BP["HIGH_CPI_BID_DECREASE_THRESH"]:
        high_cpa_keywords["new_bid"] = (high_cpa_keywords["bid"] * high_cpa_keywords["bid_multiplier_capped"]).round(2)
        no_install_keywords["new_bid"] = (
                    no_install_keywords["bid"] * no_install_keywords["bid_multiplier_capped"]).round(2)

    # combine keywords into one data frame for bid updates
    all_kws_combined = pd.concat([stale_raise_kws, low_cpa_keywords, high_cpa_keywords, no_install_keywords])

    # drop NaN new bid values resulting from portfolio bidding
    keywords_to_update_bids = all_kws_combined.dropna(subset=["new_bid"])

    # add action type column and udpate value as per Apple search api requirement
    keywords_to_update_bids["Action"] = keywords_to_update_bids.shape[0] * ["UPDATE"]

    # format for apple API specifications
    # add campaign id column as per Apple search api requirement
    keywords_to_update_bids["campaignId"] = keywords_to_update_bids.shape[0] * [campaignId]

    # subset only the columns you need
    keywords_to_update_bids = keywords_to_update_bids[["campaignId", "adGroupId", "keywordId", "new_bid"]]

    # replace name of "new_bid" to "bid"
    keywords_to_update_bids.rename(columns={"new_bid": "bid"}, inplace=True)

    # enforce min and max bid
    keywords_to_update_bids["bid"] = np.clip(keywords_to_update_bids["bid"], BP["MIN_BID"], bidCap_targetCPI)

    # send to apple
    finalized = json.loads(keywords_to_update_bids.to_json(orient="records"))
    maximum_bid = bidCap_targetCPI
    for item in finalized:
        item["bid"] = min(item["bid"], maximum_bid)  # IMPLEMENT CIRCUIT BREAKER
        summaryReportInfo[item["keywordId"]]["newBid"] = item["bid"]

    return finalized, summaryReportInfo, len(finalized)

def convertKeywordFileToApplePayload(keyword_file_to_post, currency):
    # pull the campaign and adgroup ids into an array and check if there are
    payload = [
        {
            "id": item["keywordId"],
            "bidAmount": {"currency": currency, "amount": str(item["bid"])}
        } 
        for item in keyword_file_to_post
    ]
    return payload

# quick fix for V2 update to urls TODO we can clean up to use lookup of the campaignId key to adGroupId key
def getAppleKeywordsEndpoint(keyword_file_to_post):
    url = ""
    for item in keyword_file_to_post:
        adGroupId = item["adGroupId"]
        campaignIdForEndpoint = item["campaignId"]
        url = config.APPLE_UPDATE_POSITIVE_KEYWORDS_URL % (campaignIdForEndpoint, adGroupId)
        break
    print("getAppleKeywordsEndpoint:::found url" + url)
    return url

@retry
def sendUpdatedBidsToAppleHelper(url, cert, json, headers):
    return requests.put(
        url, 
        cert=cert, 
        json=json, 
        headers=headers, 
        timeout=config.HTTP_REQUEST_TIMEOUT
    )

def sendUpdatedBidsToApple(client, keywordFileToPost):
    url = getAppleKeywordsEndpoint(keywordFileToPost)
    payload = convertKeywordFileToApplePayload(keywordFileToPost, client.currency)
    headers = {
        "Authorization": "orgId=%s" % client.orgId,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    dprint("URL is '%s'." % url)
    dprint("Payload is '%s'." % payload)
    dprint("Headers are %s." % headers)
    dprint("PEM='%s'." % client.pemFilename)
    dprint("KEY='%s'." % client.keyFilename)
     
    if (len(payload) == 0):
        print("No payload from convertKeywordFileToApplePayload. NOT actually sending anything to apple.")
        return False

    if not sendG:
        print("NOT actually sending anything to apple.")
        return False

    if sendG:
        response = sendUpdatedBidsToAppleHelper(
            url,
            cert=(S3Utils.getCert(client.pemFilename),S3Utils.getCert(client.keyFilename)),
            json=payload,
            headers=headers
        )
        if response.status_code != 200:
            email = "client id:%d \n url:%s \n response:%s" % (client.orgId, url, response)
            date = time.strftime("%m/%d/%Y")
            subject ="%s:%d ERROR in runBidAdjuster for %s" % (date, response.status_code, client.clientName)
            logger.warn(email)
            logger.error(subject)
            if sendG:
                EmailUtils.sendTextEmail(email, subject, EMAIL_TO, [], config.EMAIL_FROM)
        
        print("The result of sending the update to Apple: %s" % response)   
                   
    return sendG

def createEmailBody(data, sent):
    # Take data like this and pretty print
    # '1105630 (Covetly)': {
    #     158675458: {
    #         159571482: {
    #             'keyword': 'Funko pop chase',
    #             'newBid': 4.97664,
    #             'oldBid': '4.1472'
    #         },
    #         159571483: {
    #             'keyword': 'Funko pop track',
    #             'newBid': 4.97664,
    #             'oldBid': '4.1472'
    #         },
    #         159571484: {
    #             'keyword': 'Funko Funko Pop '
    #             'newBid': 4.97664,

    content = ["""Sent to Apple is %s.""" % sent,
               """\t""".join(["Client", "Campaign", "Keyword ID", "Keyword", "Old Bid", "New Bid"])]

    for client, clientData in data.items():
        content.append(client)
        for campaignId, campaignData in clientData.items():
            content.append("""\t\t%s""" % campaignId)
            for keywordId, keywordData in campaignData.items():
                content.append(
                    """\t\t\t\t%s\t%s\t%s\t%s""" % \
                    (
                        keywordId,
                        keywordData["keyword"],
                        keywordData["oldBid"],
                        keywordData["newBid"] if "newBid" in keywordData else "n/a"
                    )
                )

    return "\n".join(content)


def emailSummaryReport(data, sent):
    messageString = createEmailBody(data, sent);
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = "Bid Adjuster summary for %s" % dateString
    EmailUtils.sendTextEmail(messageString, subjectString, EMAIL_TO, [], config.EMAIL_FROM)


def process():
    summaryReportInfo = {}
    for client in clientsG:
        summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = clientSummaryReportInfo = {}
        campaignIds = client.campaignIds
        for campaignId in campaignIds:
            sent = False
            data = getKeywordReportFromApple(client, campaignId)
            if not data:
                logger.info("runBidAjuster:process:::no results from api:::")
                continue

            stuff = createUpdatedKeywordBids(data, campaignId, client)
            if type(stuff) != bool:
                keywordFileToPost, clientSummaryReportInfo[campaignId], numberOfUpdatedBids = stuff
                sent = sendUpdatedBidsToApple(client, keywordFileToPost)
                client.writeUpdatedBids(dynamodb, numberOfUpdatedBids) # JF unused optimization report pre-mvp
    emailSummaryReport(summaryReportInfo, sent)


@debug
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
        'body': json.dumps('Run Bid Adjuster Complete')
    }
