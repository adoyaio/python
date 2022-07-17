import logging
import decimal
from decimal import *
from datetime import datetime as dt
import datetime
import json
import pandas as pd
import requests
import time
import boto3
import traceback
import sys
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import DYNAMODB_CONTEXT #eliminate inexact and rounding errors
from botocore.exceptions import ClientError
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0
from utils.debug import debug, dprint
from utils.retry import retry
from Client import Client
from utils import DynamoUtils, S3Utils, LambdaUtils, EmailUtils
from configuration import config
from utils.DecimalEncoder import DecimalEncoder

LOOKBACK = 1
startDate = datetime.date.today() - datetime.timedelta(days=LOOKBACK)
# startDate = '2021-01-25'

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
    # NOTE uncomment to force an update to production. ie manual backfill if needed
    # dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    clientG = Client.buildFromDictionary(
        json.loads(
            clientEvent['orgDetails']
        )
    )
    authToken = clientEvent['authToken']
    logger = LambdaUtils.getLogger(clientEvent['rootEvent']['env'])
    logger.info("runAppleIntegrationCampaign:::initialize(), rootEvent='" + str(clientEvent['rootEvent']))

@retry
def getCampaignReportFromAppleHelper(url, cert, json, headers, **kw):
    # return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)
    r = requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)
    r.raise_for_status
    return r

@retry
def getCampaignReportByTokenHelper(url, json, headers, **kw):
    # return requests.post(url, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)
    r = requests.post(url, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)
    r.raise_for_status
    return r


def getCampaignReportFromApple(start_date, end_date):
    payload = {
        "startTime": str(start_date),
        "endTime": str(end_date),
        "returnRowTotals": True,
        "returnRecordsWithNoMetrics": True,
        "selector": {
            "orderBy": [{"field": "localSpend", "sortOrder": "DESCENDING"}],
            "fields": ["localSpend", "taps", "impressions", "installs", "avgCPA", "avgCPT", "ttr", "conversionRate"],
            "pagination": {"offset": 0, "limit": 1000}
        },
        # "groupBy"                    : [ "COUNTRY_CODE" ],
        # "granularity"                : 2, # 1 is hourly, 2 is daily, 3 is monthly etc
    }

    response = dict()

    # NOTE pivot on token
    if authToken is not None:
        url: str = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_KEYWORDS_REPORT_URL
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % clientG.orgId}
        dprint("URL is '%s'." % url)
        dprint("Payload is '%s'." % payload)
        dprint("Headers are %s." % headers)
        response = getCampaignReportByTokenHelper(
            url,
            json=payload,
            headers=headers
        )
    else:
        url: str = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_KEYWORDS_REPORT_URL
        headers = {"Authorization": "orgId=%s" % clientG.orgId}
        dprint("URL is '%s'." % url)
        dprint("Payload is '%s'." % payload)
        dprint("Headers are %s." % headers)
        response = getCampaignReportFromAppleHelper(
            url,
            cert=(S3Utils.getCert(clientG.pemFilename), S3Utils.getCert(clientG.keyFilename)),
            json=payload,
            headers=headers
        )
    logger.info("Response is %s." % response)

    # TODO extract to utils
    if response.status_code != 200:
        email = "client id:%d \n url:%s \n payload:%s \n response:%s" % (clientG.orgId, url, payload, response)
        date = time.strftime("%m/%d/%Y")
        subject ="%s - %d ERROR in runAppleIntegrationKeyword for %s" % (date, response.status_code, clientG.clientName)
        logger.warn(email)
        logger.error(subject)
        if sendG:
            EmailUtils.sendTextEmail(email, subject, emailToG, [], config.EMAIL_FROM)
        
        return False

    return json.loads(response.text, parse_float=decimal.Decimal)

def loadAppleCampaignToDynamo(data, orgId, endDate):
    table = dynamodb.Table('campaign_branch_history')
    rows = data["data"]["reportingDataResponse"]["row"]
    if len(rows) == 0:
        logger.debug("loadAppleCampaignToDynamo::no rows")
        return False

    for row in rows:
        # logger.debug("loadAppleKeywordToDynamo:::row:::" + str(row))

        # always pull from meta
        campaignId = str(row['metadata']['campaignId'])
        campaignName = str(row['metadata']['campaignName'])
        deleted = row['metadata']['deleted']
        campaignStatus = str(row['metadata']['campaignStatus'])
        appName = row['metadata']['app']['appName']
        adamId = str(row['metadata']['app']['adamId'])
        servingStatus = row['metadata']['servingStatus']
        modificationTime = row['metadata']['modificationTime'].split("T")[0]
        totalBudget_amount = row['metadata']['totalBudget']['amount']
        totalBudget_currency = row['metadata']['totalBudget']['currency']
        dailyBudget_amount = row['metadata']['dailyBudget']['amount']
        dailyBudget_currency = row['metadata']['dailyBudget']['currency']
        displayStatus = row['metadata']['displayStatus']

        # always pull from total
        taps = row['total']['taps']
        installs = row['total']['installs']
        ttr = row['total']['ttr']
        new_downloads = row['total']['newDownloads']
        re_downloads = row['total']['redownloads']
        lat_on_installs= row['total']['latOnInstalls']
        lat_off_installs = row['total']['latOffInstalls']
        avg_cpa = decimal.Decimal(str(row['total']['avgCPA']['amount']))
        conversion_rate = decimal.Decimal(str(row['total']['conversionRate']))
        local_spend = decimal.Decimal(str(row['total']['localSpend']['amount']))
        avg_cpt = decimal.Decimal(str(row['total']['avgCPT']['amount']))

        # TODO calc branch stats
        # NOTE incrementing on each loop thru items is unneeded as we query with timestamp, and branch data is by day at its lowest level
        branch_response = DynamoUtils.getBranchCommerceEventsByCampaign(dynamodb, campaignId, endDate)
        branch_revenue = 0
        branch_commerce_event_count = 0
        for j in branch_response.get("Items"):
            print("found branch result!")
            # print(json.dumps(j, cls=DecimalEncoder))
            branch_revenue = branch_revenue + int(j.get("revenue",0))
            branch_commerce_event_count = branch_commerce_event_count + int(j.get("count",0))

        i={
            'timestamp': str(endDate),
            'campaignId': campaignId,
            'campaignName': campaignName,
            'deleted': deleted,
            'campaignStatus': campaignStatus,
            'appName': appName,
            'adamId': adamId,
            'servingStatus': servingStatus,
            'modificationTime': modificationTime,
            'totalBudget_amount': totalBudget_amount,
            'totalBudget_currency': totalBudget_currency,
            'dailyBudget_amount': dailyBudget_amount,
            'dailyBudget_currency': dailyBudget_currency,
            'displayStatus' : displayStatus,
            'taps': taps,
            'installs': installs,
            'ttr': ttr,
            'new_downloads': new_downloads,
            're_downloads': re_downloads,
            'lat_on_installs': lat_on_installs,
            'lat_off_installs': lat_off_installs,
            'avg_cpa': avg_cpa,
            'conversion_rate': conversion_rate,
            'local_spend': local_spend,
            'avg_cpt': avg_cpt,
            'org_id' : orgId,
            'campaign_id' : str(campaignId),
            'branch_revenue' : branch_revenue,
            'branch_commerce_event_count': branch_commerce_event_count
        }
        try: 
            response = table.put_item(Item=i)
        except ClientError as e:
            logger.critical("runAppleIntegrationCampaign:::process:::PutItem failed due to" + e.response['Error']['Message'])
        else:
            logger.debug("runAppleIntegrationCampaign:::process:::PutItem succeeded:")

    return True


def export_dict_to_csv(raw_dict, filename):
    df = pd.DataFrame.from_dict(raw_dict)
    df.to_csv(filename, index=None)

def process():
    print("runAppleIntegrationCampaign:::" + clientG.clientName + ":::" + str(clientG.orgId))
    orgId = str(clientG.orgId)
    data = getCampaignReportFromApple(startDate, startDate)
    if not data:
        logger.info("runAppleIntegrationCampaign:::no data returned")

    loaded = loadAppleCampaignToDynamo(data, orgId, startDate)


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
    except Exception as e: 
        return {
            'statusCode': 400,
            'body': json.dumps('Run Apple Integration Keyword Failed: ' + str(traceback.format_exception(*sys.exc_info())))
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Apple Integration Keyword Complete for ' + clientG.clientName)
    }
