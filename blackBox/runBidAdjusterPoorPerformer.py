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
import sys
from collections import defaultdict
from datetime import datetime as dt
from boto3.dynamodb.conditions import Key, Attr
from utils.debug import debug, dprint
from utils.retry import retry
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
from Client import Client
from configuration import config
from utils.DecimalEncoder import DecimalEncoder

#SK: Configurable variable to go in clients.json--------------------------------------
cpi_bid_adjustment_poor_performer = 0.1

#SK: Changed from 14 to 2 days
BIDDING_LOOKBACK = 2
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
        "runBidAdjuster:::initialize(), rootEvent='" + str(clientEvent['rootEvent'])
    )

def getKeywordReportFromAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

def getKeywordReportByTokenHelper(url,  json, headers):
    return requests.post(url, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

def getKeywordReportFromApple(campaignId):
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
            "conditions": [
                {
                    "field": "keywordStatus",
                    "operator": "IN",
                    "values": [
                        "ACTIVE"
                    ]
                },
                {
                    "field": "keywordDisplayStatus",
                    "operator": "IN",
                    "values": [
                        "RUNNING"
                    ]
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

    response = dict()

    # NOTE pivot on token until v3 sunset
    if authToken is not None:
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaignId
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % clientG.orgId}
        dprint("\nURL is %s" % url)
        dprint("\nPayload is %s" % payload)
        dprint("\nHeaders are %s" % headers)
        response = getKeywordReportByTokenHelper(
            url,
            json=payload,
            headers=headers
        )
    else:
        url = config.APPLE_SEARCHADS_URL_BASE_V3 + config.APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaignId
        headers = {"Authorization": "orgId=%s" % clientG.orgId}
        dprint("\nURL is %s" % url)
        dprint("\nPayload is %s" % payload)
        dprint("\nHeaders are %s" % headers)
        response = getKeywordReportFromAppleHelper(
            url,
            cert=(S3Utils.getCert(clientG.pemFilename),S3Utils.getCert(clientG.keyFilename)),
            json=payload,
            headers=headers
        )
        
    dprint("Response is %s" % response)

    if response.status_code != 200:
        email = "client id:%d \n url:%s \n payload:%s \n response:%s" % (clientG.orgId, url, payload, response)
        date = time.strftime("%m/%d/%Y")
        subject ="%s - %d ERROR in runBidAdjuster for %s" % (date, response.status_code, clientG.clientName)
        logger.warn(email)
        logger.error(subject)
        if sendG:
            EmailUtils.sendTextEmail(email, subject, emailToG, [], config.EMAIL_FROM)
        
        return False

    return json.loads(response.text)
   
def createUpdatedKeywordBids(data, campaign, BP):
    rows = data["data"]["reportingDataResponse"]["row"] 
    if len(rows) == 0:
        return False

    keyword_info = defaultdict(list)
    summaryReportInfo = {}
    HIGH_CPI_BID_DECREASE_THRESH = BP["HIGH_CPI_BID_DECREASE_THRESH"]

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

    # check if branch integration is enabled, if so only update bids on keywords with installs < min_apple_installs
    branch_bid_adjuster_enabled = clientG.branchIntegrationParameters.get("branch_bid_adjuster_enabled", False)

    if branch_bid_adjuster_enabled:
        ex_keyword_info = ex_keyword_info[ex_keyword_info["installs"] < (clientG.branchBidParameters["min_apple_installs"])]

    # first convert avg cpa to float so you can perform calculations
    ex_keyword_info["avgCPA"] = ex_keyword_info["avgCPA"].astype(float)
    ex_keyword_info["bid"] = ex_keyword_info["bid"].astype(float)

    # calculate bid multiplier and create a new column
    ex_keyword_info["bid_multiplier"] = HIGH_CPI_BID_DECREASE_THRESH / ex_keyword_info["avgCPA"]

# create upper bid cap tied to target cost per install.
    if BP["OBJECTIVE"] == "aggressive":
        bidCap_targetCPI = HIGH_CPI_BID_DECREASE_THRESH * 1.20
    elif BP["OBJECTIVE"] == "standard":
        bidCap_targetCPI = HIGH_CPI_BID_DECREASE_THRESH * 1
    elif BP["OBJECTIVE"] == "conservative":
        bidCap_targetCPI = HIGH_CPI_BID_DECREASE_THRESH * 0.80
    else:
        print("no objective selected default to standard")
        bidCap_targetCPI = HIGH_CPI_BID_DECREASE_THRESH * 1
    
    high_cpa_keywords = ex_keyword_info[
        (ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
        (ex_keyword_info["avgCPA"] > (HIGH_CPI_BID_DECREASE_THRESH * 2)) & \
        (ex_keyword_info["installs"] > BP["NO_INSTALL_BID_DECREASE_THRESH"])
    ]

    no_install_keywords = ex_keyword_info[
        (ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
        (ex_keyword_info["installs"] == BP["NO_INSTALL_BID_DECREASE_THRESH"])
    ]

    high_cpa_keywords["new_bid"] = (high_cpa_keywords["bid"] * (1 - cpi_bid_adjustment_poor_performer)).round(2)
    no_install_keywords["new_bid"] = (no_install_keywords["bid"] * (1 - cpi_bid_adjustment_poor_performer)).round(2)

    # combine keywords into one data frame for bid updates
    all_kws_combined = pd.concat([high_cpa_keywords, no_install_keywords])
    #all_kws_combined = pd.concat([no_install_keywords])

    # drop NaN new bid values resulting from portfolio bidding
    keywords_to_update_bids = all_kws_combined.dropna(subset=["new_bid"])

    # add action type column and udpate value as per Apple search api requirement
    keywords_to_update_bids["Action"] = keywords_to_update_bids.shape[0] * ["UPDATE"]

    # format for apple API specifications
    # add campaign id column as per Apple search api requirement
    keywords_to_update_bids["campaignId"] = keywords_to_update_bids.shape[0] * [campaign["campaignId"]]

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
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_UPDATE_POSITIVE_KEYWORDS_URL % (campaignIdForEndpoint, adGroupId)
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

@retry
def sendUpdatedBidsByTokenHelper(url, json, headers):
    return requests.put(
        url, 
        json=json, 
        headers=headers, 
        timeout=config.HTTP_REQUEST_TIMEOUT
    )

def sendUpdatedBidsToApple(keywordFileToPost):
    # TODO cleanup getAppleKeywordsEndpoint, pull campaign and adgroup ids from the apple campaign object
    url = getAppleKeywordsEndpoint(keywordFileToPost)
    payload = convertKeywordFileToApplePayload(keywordFileToPost, clientG.currency)
    
    # NOTE pivot on token until v3 sunset
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

    dprint("URL is '%s'." % url)
    dprint("Payload is '%s'." % payload)
    dprint("Headers are %s." % headers)
    dprint("PEM='%s'." % clientG.pemFilename)
    dprint("KEY='%s'." % clientG.keyFilename)
     
    if (len(payload) == 0):
        print("No payload from convertKeywordFileToApplePayload. NOT actually sending anything to apple.")
        return False

    if not sendG:
        print("NOT actually sending anything to apple.")
        return False

    if sendG:
        if authToken is not None:
            response = sendUpdatedBidsByTokenHelper(
                url,
                json=payload,
                headers=headers
            )
        else:
            response = sendUpdatedBidsToAppleHelper(
                url,
                cert=(S3Utils.getCert(clientG.pemFilename),S3Utils.getCert(clientG.keyFilename)),
                json=payload,
                headers=headers
            )
            
        if response.status_code != 200:
            email = "client id:%d \n url:%s \n response:%s" % (clientG.orgId, url, response)
            date = time.strftime("%m/%d/%Y")
            subject ="%s:%d ERROR in runBidAdjuster for %s" % (date, response.status_code, clientG.clientName)
            logger.warn(email)
            logger.error(subject)
            EmailUtils.sendTextEmail(email, subject, emailToG, [], config.EMAIL_FROM)
        
        print("The result of sending the update to Apple: %s" % response)   
                   
    return True

def createEmailBody(data, sent):
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
    subjectString = "%s - Bid Adjuster summary for %s" % (clientG.clientName, dateString)
    EmailUtils.sendTextEmail(messageString, subjectString, emailToG, [], config.EMAIL_FROM)


def process():
    print("runBidAdjusterPoorPerformer:::" + clientG.clientName + ":::" + str(clientG.orgId))
    summaryReportInfo = {}
    summaryReportInfo["%s (%s)" % (clientG.orgId, clientG.clientName)] = clientSummaryReportInfo = {} 
    appleCampaigns = clientG.appleCampaigns
    campaignsForBidAdjuster = list(
        filter(
            lambda campaign:(campaign["bidAdjusterEnabled"] == True), appleCampaigns
        )
    )
    for campaign in campaignsForBidAdjuster:
        sent = False
        bidParameters = LambdaUtils.getBidParamsForJob(clientG.__dict__, campaign, "bidAdjuster")
        print("bidParameters" + str(bidParameters))
        data = getKeywordReportFromApple(campaign['campaignId'])
        if not data:
            logger.info("runBidAdjusterPoorPerformer:process:::no results from api:::")
            continue
        
        stuff = createUpdatedKeywordBids(
            data, 
            campaign,
            bidParameters
        )
        if type(stuff) != bool:
            keywordFileToPost, clientSummaryReportInfo[campaign['campaignId']], numberOfUpdatedBids = stuff
            sent = sendUpdatedBidsToApple(keywordFileToPost)

    emailSummaryReport(summaryReportInfo, sent)


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
            'body': json.dumps('Run Bid Adjuster Poor Performer Failed')
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Bid Adjuster Poor Performer Complete for ' + clientG.clientName)
    }