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

LOOKBACK = 14 # TODO reduce for nightly

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
    logger.info("runAppleIntegrationKeyword:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))

@retry
def getKeywordReportFromAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)


def getKeywordReportFromApple(client, campaign_id, start_date, end_date):
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

    url = config.APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaign_id
    headers = {"Authorization": "orgId=%s" % client.orgId}
    dprint("URL is '%s'." % url)
    dprint("Payload is '%s'." % payload)
    dprint("Headers are %s." % headers)
    response = getKeywordReportFromAppleHelper(
        url,
        cert=(S3Utils.getCert(client.pemFilename), S3Utils.getCert(client.keyFilename)),
        json=payload,
        headers=headers
    )
    logger.info("Response is %s." % response)

    # TODO extract to utils
    if response.status_code != 200:
        email = "client id:%d \n url:%s \n payload:%s \n response:%s" % (client.orgId, url, payload, response)
        date = time.strftime("%m/%d/%Y")
        subject ="%s - %d ERROR in runAppleIntegrationKeyword for %s" % (date, response.status_code, client.clientName)
        logger.warn(email)
        logger.error(subject)
        if sendG:
            EmailUtils.sendTextEmail(email, subject, EMAIL_TO, [], config.EMAIL_FROM)
        
        return False

    return json.loads(response.text, parse_float=decimal.Decimal)

def loadAppleKeywordToDynamo(data, orgId, campaignId):
    table = dynamodb.Table('apple_keyword')
    rows = data["data"]["reportingDataResponse"]["row"]
    if len(rows) == 0:
        logger.debug("loadAppleKeywordToDynamo::no rows")
        return False

    for row in rows:
            logger.debug("loadAppleKeywordToDynamo:::row:::" + str(row))
            if "total" in row.keys():
                field_key = "total"
            else:
                field_key = "granularity"

            # print("loadAppleKeywordToDynamo:::field_key:::" + field_key)
            for granularity in row["granularity"]:
                logger.debug("granularity:" + str(granularity))

                # always pull date from granularity
                date = str(granularity['date'])

                # always pull from meta
                keyword = str(row['metadata']['keyword'])
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
    for client in clientsG:
        print("runAppleIntegrationKeyword:::" + client.clientName + ":::" + str(client.orgId))
        orgId = str(client.orgId)
        campaignIds = client.campaignIds
        for campaignId in campaignIds:
                # TODO JF implement max date call pull ONE value sorted & read max date
                # date_results = keyword_table.scan(FilterExpression=Key('campaignId').eq(str(campaignId)))
                startDate = datetime.date.today() - datetime.timedelta(days=LOOKBACK)
                endDate = datetime.date.today()
                # print("start_date:::" + str(start_date))
                # print("end_date::: " + str(end_date))
                data = getKeywordReportFromApple(client, campaignId, startDate, endDate)
                if not data:
                    logger.info("runAppleIntegrationKeyword:::no data returned")
                    continue

                loaded = loadAppleKeywordToDynamo(data, orgId, campaignId)

def terminate():
    pass


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
        'body': json.dumps('Run Apple Integration Keyword Complete')
    }
