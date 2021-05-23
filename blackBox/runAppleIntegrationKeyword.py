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

LOOKBACK = 7 # TODO reduce for nightly
startDate = datetime.date.today() - datetime.timedelta(days=LOOKBACK)
endDate = datetime.date.today()
# startDate = '2021-01-25'
# endDate = '2021-01-31'

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
    logger.info("runAppleIntegrationKeyword:::initialize(), rootEvent='" + str(clientEvent['rootEvent']))

@retry
def getKeywordReportFromAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

@retry
def getKeywordReportByTokenHelper(url, json, headers):
    return requests.post(url, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)


def getKeywordReportFromApple(campaign_id, start_date, end_date):
    payload = {
        "startTime": str(start_date),
        "endTime": str(end_date),
        "timeZone": "ORTZ",
        "granularity": "DAILY",
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
        "returnRowTotals": False,
        "returnRecordsWithNoMetrics": True
    }

    response = dict()

    # NOTE pivot on token
    if authToken is not None:
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaign_id
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % clientG.orgId}
        dprint("URL is '%s'." % url)
        dprint("Payload is '%s'." % payload)
        dprint("Headers are %s." % headers)
        response = getKeywordReportByTokenHelper(
            url,
            json=payload,
            headers=headers
        )
    else:
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaign_id
        headers = {"Authorization": "orgId=%s" % clientG.orgId}
        dprint("URL is '%s'." % url)
        dprint("Payload is '%s'." % payload)
        dprint("Headers are %s." % headers)
        response = getKeywordReportFromAppleHelper(
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

def loadAppleKeywordToDynamo(data, orgId, campaignId):
    table = dynamodb.Table('apple_keyword')
    rows = data["data"]["reportingDataResponse"]["row"]
    if len(rows) == 0:
        # logger.debug("loadAppleKeywordToDynamo::no rows")
        return False

    for row in rows:
        # logger.debug("loadAppleKeywordToDynamo:::row:::" + str(row))
        if "total" in row.keys():
            field_key = "total"
        else:
            field_key = "granularity"

        # print("loadAppleKeywordToDynamo:::field_key:::" + field_key)
        for granularity in row["granularity"]:
            # logger.debug("granularity:" + str(granularity))

            # always pull date from granularity
            date = str(granularity['date'])

            # always pull from meta
            keyword = str(row['metadata']['keyword']).lower()
            keyword_id = str(row['metadata']['keywordId'])
            keywordStatus = row['metadata']['keywordStatus']
            keywordDisplayStatus = row['metadata']['keywordDisplayStatus']
            matchType = row['metadata']['matchType']
            adgroup_name = row['metadata']['adGroupName']
            adgroup_id = str(row['metadata']['adGroupId'])
            adgroup_deleted = str(row['metadata']['adGroupDeleted'])
            bid = decimal.Decimal(str(row['metadata']['bidAmount']['amount']))
            deleted = row['metadata']['deleted']
            modification_time = row['metadata']['modificationTime'].split("T")[0]

            if field_key == "total":
                impressions = row[field_key]['impressions']
                taps = row[field_key]['taps']
                installs = row[field_key]['installs']
                ttr = row[field_key]['ttr']
                new_downloads = row[field_key]['newDownloads']
                re_downloads = row[field_key]['redownloads']
                lat_on_installs= row[field_key]['latOnInstalls']
                lat_off_installs = row[field_key]['latOffInstalls']
                avg_cpa = decimal.Decimal(str(row[field_key]['avgCPA']['amount']))
                conversion_rate = decimal.Decimal(str(row[field_key]['conversionRate']))
                local_spend = decimal.Decimal(str(row[field_key]['localSpend']['amount']))
                avg_cpt = decimal.Decimal(str(row[field_key]['avgCPT']['amount']))

            else:
                impressions = granularity['impressions']
                taps = granularity['taps']
                installs = granularity['installs']
                ttr = granularity['ttr']
                new_downloads = granularity['newDownloads']
                re_downloads = granularity['redownloads']
                lat_on_installs = granularity['latOnInstalls']
                lat_off_installs = granularity['latOffInstalls']
                avg_cpa = decimal.Decimal(str(granularity['avgCPA']['amount']))
                conversion_rate = decimal.Decimal(str(granularity['conversionRate']))
                local_spend = decimal.Decimal(str(granularity['localSpend']['amount']))
                avg_cpt = decimal.Decimal(str(granularity['avgCPT']['amount']))

            i={
                'date': date,
                'keyword': keyword,
                'keyword_id': keyword_id,
                'keywordStatus': keywordStatus,
                'keywordDisplayStatus': keywordDisplayStatus,
                'matchType': matchType,
                'adgroup_name': adgroup_name,
                'adgroup_id' : adgroup_id,
                'adgroup_deleted': adgroup_deleted,
                'bid': bid,
                'deleted': deleted,
                'modification_time': modification_time,
                'impressions': impressions,
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
                'campaign_id' : campaignId
            }

            try: 
                response = table.put_item(Item=i)
            except ClientError as e:
                logger.critical("runAppleIntegrationKeyword:::process:::PutItem failed due to" + e.response['Error']['Message'])
            else:
                logger.debug("runAppleIntegrationKeyword:::process:::PutItem succeeded:")

    return True


def export_dict_to_csv(raw_dict, filename):
    df = pd.DataFrame.from_dict(raw_dict)
    df.to_csv(filename, index=None)

def process():
    print("runAppleIntegrationKeyword:::" + clientG.clientName + ":::" + str(clientG.orgId))
    orgId = str(clientG.orgId)
    appleCampaigns = clientG.appleCampaigns
    campaignsForAppleKeywordIntegration = list(
        filter(
            lambda campaign:(
                campaign["keywordIntegrationEnabled"] == True
            ),
            appleCampaigns
        )
    )
    for campaign in campaignsForAppleKeywordIntegration:
        # TODO JF implement max date call pull ONE value sorted & read max date
        # date_results = keyword_table.scan(FilterExpression=Key('campaignId').eq(str(campaignId)))
        data = getKeywordReportFromApple(campaign['campaignId'], startDate, endDate)
        if not data:
            logger.info("runAppleIntegrationKeyword:::no data returned")
            continue

        loaded = loadAppleKeywordToDynamo(data, orgId, campaign['campaignId'])


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
            'body': json.dumps('Run Apple Integration Keyword Failed')
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Apple Integration Keyword Complete for ' + clientG.clientName)
    }
