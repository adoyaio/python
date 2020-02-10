#! /usr/bin/python3
import decimal
from collections import defaultdict
from datetime import datetime as dt
import datetime
import json
import pandas as pd
import pprint
import requests
import time
import boto3
from utils import AdoyaEmail
from boto3.dynamodb.conditions import Key, Attr

# TODO rm v0 code
# if sys.platform == "linux":
#   os.chdir("/Users/james/Documents/adoya/python")
#
# else:
#   PATH_ADDEND = "C:\\Users\A\\Desktop\\apple_search_ads_api\\bid_adjuster"
#   print("Adding '%s' to sys.path." % PATH_ADDEND);
#   sys.path.append(PATH_ADDEND)


from Client import CLIENTS
from configuration import EMAIL_FROM, \
    APPLE_UPDATE_POSITIVE_KEYWORDS_URL, \
    APPLE_KEYWORD_REPORTING_URL_TEMPLATE, \
    TOTAL_COST_PER_INSTALL_LOOKBACK, \
    HTTP_REQUEST_TIMEOUT

from debug import debug, dprint
from retry import retry
import logging

BIDDING_LOOKBACK = 7  # days

###### date and time parameters for bidding lookback ######
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta

# FOR QA PURPOSES set these fields explicitly
#start_date = dt.strptime('2019-12-01', '%Y-%m-%d').date()
#end_date = dt.strptime('2019-12-08', '%Y-%m-%d').date()

# url to api server for keywords report
# From https://developer.apple.com/library/archive/documentation/General/Conceptual/AppStoreSearchAdsAPIReference/Reporting_Methods.html:
# POST /v1/reports/campaigns/<CAMPAIGN_ID>/keywords
# 
# Get reports on targeted keywords within a specific adgroup.
# 
#     curl \
#        -H ...\
#        -d "@TestKeywordReport.json"
#        -X POST "<ROOT_PATH>/v1/reports/campaigns/{cId}/keywords"
# 
#     By default, soft deleted targeted keywords, which belong to soft deleted ad groups, are not returned
# 
#     Selectors can be used to also get reporting on soft deleted keywords
# 
#     Grouping by countryCode, adminArea, deviceClass, ageRange, or gender are not supported for this endpoint.
# From https://developer.apple.com/library/archive/documentation/General/Conceptual/AppStoreSearchAdsAPIReference/Keyword_Methods.html:
# POST /v1/keywords/targeting
# 
# Create or update a list of targeted keywords within a specific org.
# 
#    curl \
#      -d @testUpdateTargetedKeywords.json
#      -X POST "https://api.searchads.apple.com/api/v1/keywords/targeting"


sendG = False  # Set to True to enable sending data to Apple, else a test run.
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


@debug
def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global dynamodb
    global EMAIL_TO

    EMAIL_TO = emailToInternal
    if env != "prod":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
    else:
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    logger.info("In runBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s', emailTo='%s'" % (sendG, dynamoEndpoint, str(EMAIL_TO)))


# ------------------------------------------------------------------------------
@retry
def getKeywordReportFromAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)


# ------------------------------------------------------------------------------
@debug
def getKeywordReportFromApple(client, campaignId):
    payload = {"startTime": str(start_date),
               "endTime": str(end_date),
               # "granularity"                : 2, # 1=hourly, 2=daily, 3=monthly, etc.
               "selector": {"orderBy": [{"field": "localSpend",
                                         "sortOrder": "DESCENDING"
                                         }],
                            "fields": ["localSpend",
                                       "taps",
                                       "impressions",
                                       "conversions",
                                       "avgCPA",
                                       "avgCPT",
                                       "ttr",
                                       "conversionRate"
                                       ],
                            "pagination": {"offset": 0,
                                           "limit": 1000
                                           }
                            },
               # "groupBy"                    : ["COUNTRY_CODE"],
               "returnRowTotals": True,
               "returnRecordsWithNoMetrics": True
               }
    url = APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaignId

    headers = {"Authorization": "orgId=%s" % client.orgId}

    dprint("URL is '%s'." % url)
    dprint("Payload is '%s'." % payload)
    dprint("Headers are %s." % headers)

    response = getKeywordReportFromAppleHelper(url,
                                               cert=(client.pemPathname, client.keyPathname),
                                               json=payload,
                                               headers=headers)
    dprint("Response is %s." % response)

    return json.loads(response.text)


# ------------------------------------------------------------------------------
@debug
def createUpdatedKeywordBids(data, campaignId, client):
    rows = data["data"]["reportingDataResponse"]["row"]

    if len(rows) == 0:
        return False  # EARLY RETURN

    keyword_info = defaultdict(list)
    summaryReportInfo = {}

    for row in rows:
        metadata = row["metadata"]

        summaryReportInfo[metadata["keywordId"]] = {"keyword": metadata["keyword"],
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
        # keyword_info["other"]                  .append(metadata["other"])

        totals = row["total"]

        keyword_info["impressions"].append(totals["impressions"])
        keyword_info["taps"].append(totals["taps"])
        keyword_info["ttr"].append(totals["ttr"])
        keyword_info["conversions"].append(totals["conversions"])
        keyword_info["conversionsNewDownloads"].append(totals["conversionsNewDownloads"])
        keyword_info["conversionsRedownloads"].append(totals["conversionsRedownloads"])
        keyword_info["conversionsLATOn"].append(totals["conversionsLATOn"])
        keyword_info["conversionsLATOff"].append(totals["conversionsLATOff"])
        keyword_info["avgCPA"].append(totals["avgCPA"]["amount"])
        keyword_info["conversionRate"].append(totals["conversionRate"])
        keyword_info["localSpend"].append(totals["localSpend"]["amount"])
        keyword_info["avgCPT"].append(totals["avgCPT"]["amount"])

    dprint("keyword_info=%s." % pprint.pformat(keyword_info))

    # convert to dataframe
    df_keyword_info = pd.DataFrame(keyword_info)

    dprint("df_keyword_info=%s." % str(df_keyword_info))

    # pull in active keywords only
    # Changed, 15-Sep-18 per email from Scott Kaplan of 8-Sep-18
    #  keyword_info = keyword_info[keyword_info['keywordStatus'] == 'ACTIVE']
    df_keyword_info = df_keyword_info[df_keyword_info['keywordDisplayStatus'] == 'RUNNING']

    # extract only the columns you need for keyword bids
    ex_keyword_info = df_keyword_info[["keyword",
                                       "matchType",
                                       "adGroupId",
                                       "keywordId",
                                       "impressions",
                                       "taps",
                                       "conversions",
                                       "avgCPA",
                                       "localSpend",
                                       "bid"]]

    dprint("ex_keyword_info=%s." % str(ex_keyword_info))

    ######make bid adjustments######

    BP = client.bidParameters;

    # first convert avg cpa to float so you can perform calculations
    ex_keyword_info["avgCPA"] = ex_keyword_info["avgCPA"].astype(float)
    ex_keyword_info["bid"] = ex_keyword_info["bid"].astype(float)

    # subset keywords for stale raises
    stale_raise_kws = ex_keyword_info[ex_keyword_info["taps"] < BP["TAP_THRESHOLD"]]

    # subset keywords for bid increase
    low_cpa_keywords = ex_keyword_info[(ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
                                       (ex_keyword_info["avgCPA"] <= BP["HIGH_CPI_BID_DECREASE_THRESH"]) & \
                                       (ex_keyword_info["conversions"] > BP["NO_INSTALL_BID_DECREASE_THRESH"])]

    # subset keywords for bid decrease
    high_cpa_keywords = ex_keyword_info[(ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
                                        (ex_keyword_info["avgCPA"] > BP["HIGH_CPI_BID_DECREASE_THRESH"])]
    no_install_keywords = ex_keyword_info[(ex_keyword_info["taps"] >= BP["TAP_THRESHOLD"]) & \
                                          (ex_keyword_info["conversions"] == BP["NO_INSTALL_BID_DECREASE_THRESH"])]

    # raise bids for stale raise keywords
    stale_raise_kws["bid"] = stale_raise_kws["bid"] * BP["STALE_RAISE_BID_BOOST"]

    # raise bids for low cpi keywords
    low_cpa_keywords["bid"] = low_cpa_keywords["bid"] * BP["LOW_CPA_BID_BOOST"]

    # check if overall CPI is within bid threshold, if not, fix it.
    total_cost_per_install = client.getTotalCostPerInstall(dynamodb, start_date, end_date,
                                                           TOTAL_COST_PER_INSTALL_LOOKBACK)
    dprint("total cpi %s" % str(total_cost_per_install))

    if total_cost_per_install > BP["HIGH_CPI_BID_DECREASE_THRESH"]:
        high_cpa_keywords["bid"] = high_cpa_keywords["bid"] * BP["HIGH_CPA_BID_DECREASE"]
        no_install_keywords["bid"] = no_install_keywords["bid"] * BP["HIGH_CPA_BID_DECREASE"]

    # combine keywords into one data frame for bid updates
    keywords_to_update_bids = [stale_raise_kws, low_cpa_keywords, high_cpa_keywords, no_install_keywords]
    keywords_to_update_bids = pd.concat(keywords_to_update_bids)
    # add action type column and udpate value as per Apple search api requirement
    keywords_to_update_bids["Action"] = keywords_to_update_bids.shape[0] * ["UPDATE"]

    # add campaign id column as per Apple search api requirement
    keywords_to_update_bids["campaignId"] = keywords_to_update_bids.shape[0] * [campaignId]

    result_1 = json.loads(keywords_to_update_bids.to_json(orient="records"))
    maximum_bid = BP["MAX_BID"]
    for item in result_1:
        item["bid"] = min(item["bid"], maximum_bid)  # IMPLEMENT CIRCUIT BREAKER
        summaryReportInfo[item["keywordId"]]["newBid"] = item["bid"]

    return result_1, summaryReportInfo, len(result_1)


# ------------------------------------------------------------------------------
@debug
def convertKeywordFileToApplePayload(keyword_file_to_post):
    '''
At  https://developer.apple.com/library/archive/documentation/General/Conceptual/AppStoreSearchAdsAPIReference/Keyword_Resources.html I see this:

Bulk Targeted Keywords JSON Representation

    { "importAction" : enum,
      "id"           : number,
      "campaignId"   : number,
      "adGroupId"    : number,
      "text"         : string,
      "matchType"    : enum,
      "status"       : enum,
      "deleted"      : boolean,
      "bidAmount"    : {Amount object}
    }

The last field, bidAmount, is an "Amount object." At https://developer.apple.com/library/archive/documentation/General/Conceptual/AppStoreSearchAdsAPIReference/Campaign_Resources.html, an Amount object is defined as { "currency": string, "amount": string }

(I see something different at https://developer.apple.com/library/archive/documentation/General/Conceptual/AppStoreSearchAdsAPIReference/Campaign_Resources.html.)

The keyword_file_to_post parameter is an array of these objects:

   {  "keyword"		: "freelancing time",
      "matchType"	: "BROAD",
      "adGroupId"	: 152710576,
      "keywordId"	: 152834423,
      "impressions"	: 0,
      "taps"		: 0,
      "conversions"	: 0,
      "avgCPA"		: 0.0,
      "localSpend"	: "0",
      "bid"			: 12.0,
      "Action"		: "UPDATE",
      "campaignId"	: 152708992  }
  '''

    payload = [{"importAction": "UPDATE",
                "id": item["keywordId"],
                "campaignId": item["campaignId"],
                "adGroupId": item["adGroupId"],
                "bidAmount": {"currency": "USD", "amount": str(item["bid"])}
                } for item in keyword_file_to_post]

    return payload


# ------------------------------------------------------------------------------
@retry
def sendUpdatedBidsToAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)


# ------------------------------------------------------------------------------
@debug
def sendUpdatedBidsToApple(client, keywordFileToPost):
    payload = convertKeywordFileToApplePayload(keywordFileToPost)

    headers = {"Authorization": "orgId=%s" % client.orgId,
               "Content-Type": "application/json",
               "Accept": "application/json",
               }

    dprint("URL is '%s'." % APPLE_UPDATE_POSITIVE_KEYWORDS_URL)
    dprint("Payload is '%s'." % payload)
    dprint("Headers are %s." % headers)
    dprint("PEM='%s'." % client.pemPathname)
    dprint("KEY='%s'." % client.keyPathname)

    if sendG:
        response = sendUpdatedBidsToAppleHelper(APPLE_UPDATE_POSITIVE_KEYWORDS_URL,
                                                cert=(client.pemPathname, client.keyPathname),
                                                json=payload,
                                                headers=headers)

    else:
        response = "Not actually sending anything to Apple."

    print("The result of sending the update to Apple: %s" % response)

    return sendG


# ------------------------------------------------------------------------------
@debug
def createEmailBody(data, sent):
    """Take data like this:

'1105630 (Covetly)': {158675458: {159571482: {'keyword': 'Funko pop chase',
                                               'newBid': 4.97664,
                                               'oldBid': '4.1472'},
                                   159571483: {'keyword': 'Funko pop track',
                                               'newBid': 4.97664,
                                               'oldBid': '4.1472'},
                                   159571484: {'keyword': 'Funko Funko Pop '
                                                          'Funko pop buy',
                                               'newBid': 4.97664,

  and convert it into an HTML table.
"""

    content = ["""Sent to Apple is %s.""" % sent,
               """\t""".join(["Client", "Campaign", "Keyword ID", "Keyword", "Old Bid", "New Bid"])]

    for client, clientData in data.items():
        content.append(client)
        for campaignId, campaignData in clientData.items():
            content.append("""\t\t%s""" % campaignId)
            for keywordId, keywordData in campaignData.items():
                # TODO: Put the new bid in red if it is a decrease from the old bid. --DS, 13-Oct-2018
                content.append("""\t\t\t\t%s\t%s\t%s\t%s""" % \
                               (keywordId,
                                keywordData["keyword"],
                                keywordData["oldBid"],
                                keywordData["newBid"] if "newBid" in keywordData else "n/a"))

    return "\n".join(content)


# ------------------------------------------------------------------------------
@debug
def emailSummaryReport(data, sent):
    messageString = createEmailBody(data, sent);
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString =  "Bid Adjuster summary for %s" % dateString
    AdoyaEmail.sendEmailForACampaign(messageString, subjectString, EMAIL_TO, EMAIL_FROM)

# ------------------------------------------------------------------------------
@debug
def process():
    summaryReportInfo = {}

    for client in CLIENTS:
        summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = clientSummaryReportInfo = {}
        campaignIds = client.campaignIds

        for campaignId in campaignIds:
            data = getKeywordReportFromApple(client, campaignId)

            stuff = createUpdatedKeywordBids(data, campaignId, client)

            if type(stuff) != bool:
                keywordFileToPost, clientSummaryReportInfo[campaignId], numberOfUpdatedBids = stuff
                sent = sendUpdatedBidsToApple(client, keywordFileToPost)
                #if sent:
                #client.updatedBids = numberOfUpdatedBids
                client.updatedBids(dynamodb, numberOfUpdatedBids)

    emailSummaryReport(summaryReportInfo, sent)


# ------------------------------------------------------------------------------
@debug
def terminate():
    pass


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    initialize('lcl', 'http://localhost:8000', ["test@adoya.io"])
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
