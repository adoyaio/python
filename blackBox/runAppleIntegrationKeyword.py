import logging
import decimal
from decimal import *
from datetime import datetime as dt
import datetime
import json
import pandas as pd
import requests
import boto3
from boto3.dynamodb.conditions import Key
# this was to eliminate the inexact and rounding errors
from boto3.dynamodb.types import DYNAMODB_CONTEXT
# Inhibit Inexact Exceptions
from botocore.exceptions import ClientError
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

from debug import debug, dprint
from retry import retry
from Client import Client
from utils import DynamoUtils, S3Utils
from configuration import config

LOOKBACK = 14 # TODO reduce this for nightly
sendG = False  #enable data to Apple, else a test run
logger = logging.getLogger()

def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global dynamodb
    global EMAIL_TO
    global clientsG
    EMAIL_TO = emailToInternal

    if env == "lcl":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
        logger.setLevel(logging.INFO)
    elif env == "prod":
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)  # reduce AWS logging in production
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)

    clientsG = Client.getClients(dynamodb)
    logger.info("runAppleIntegrationKeyword:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


@retry
def getKeywordReportFromAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)


def getKeywordReportFromApple(client, campaign_id, start_date, end_date):
    """The data from Apple looks like this (Pythonically):
    {'data': {'reportingDataResponse': {'row': [{'metadata': {'adGroupDisplayStatus': 'CAMPAIGN_ON_HOLD',
                                                            'adGroupId': 152725486,
                                                            'adGroupName': 'search_match',
                                                            'adGroupServingStateReasons': None,
                                                            'adGroupServingStatus': 'RUNNING',
                                                            'adGroupStatus': 'ENABLED',
                                                            'automatedKeywordsOptIn': True,
                                                            'cpaGoal': {'amount': '1',
                                                                        'currency': 'USD'},
                                                            'defaultCpcBid': {'amount': '0.5',
                                                                              'currency': 'USD'},
                                                            'deleted': False,
                                                            'endTime': None,
                                                            'modificationTime': '2018-08-29T07:58:51.872',
                                                            'startTime': '2018-05-26T00:00:00.000'},
                                               'other': False,
                                               'total': {'avgCPA': {'amount': '0',
                                                                    'currency': 'USD'},
                                                         'avgCPT': {'amount': '0',
                                                                    'currency': 'USD'},
                                                         'conversionRate': 0.0,
                                                         'installs': 0,
                                                         'latOffInstalls': 0,
                                                         'latOnInstalls': 0,
                                                         'newDownloads': 0,
                                                         'redownloads': 0,
                                                         'impressions': 0,
                                                         'localSpend': {'amount': '0',
                                                                        'currency': 'USD'},
                                                         'taps': 0,
                                                         'ttr': 0.0}}]}},
    'error': None,
    'pagination': {'itemsPerPage': 1, 'startIndex': 0, 'totalResults': 1}}
    """

    payload = {"startTime": str(start_date),
               "endTime": str(end_date),
               "timeZone": "ORTZ",
               "granularity": "DAILY",
               "selector": {"orderBy": [{"field": "localSpend",
                                         "sortOrder": "DESCENDING"
                                         }],
                            "fields": ["localSpend",
                                       "taps",
                                       "impressions",
                                       "installs",
                                       "avgCPA",
                                       "avgCPT",
                                       "ttr",
                                       "conversionRate"
                                       ],
                            "pagination": {"offset": 0,
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

    response = getKeywordReportFromAppleHelper(url,
                                               cert=(S3Utils.getCert(client.pemFilename),
                                                     S3Utils.getCert(client.keyFilename)),
                                               json=payload,
                                               headers=headers)

    print("runAppleIntegrationKeyword:::Response is " + str(response))
    if response.status_code == 200:
        return json.loads(response.text, parse_float=decimal.Decimal)
    else:
        return False

def loadAppleKeywordToDynamo(data, keyword_table):
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

            logger.debug("loadAppleKeywordToDynamo:::field_key:::" + field_key)
            
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

                    # enable for local debugging
                    # dprint("date=%s" % date)
                    # dprint("impressions=%s" % impressions)
                    # dprint("taps=%s" % taps)
                    # dprint("installs=%s" % installs)
                    # dprint("ttr=%s" % ttr)
                    # dprint("new_downloads=%s" % new_downloads)
                    # dprint("re_downloads=%s" % re_downloads)
                    # dprint("avg_cpt=%s" % avg_cpt)
                    # print("avg_cpt" + str(avg_cpt))
                    # print("local_spend" + str(local_spend))
                    # print("conversion_rate" + str(conversion_rate))
                    # print("avg_cpa" + str(avg_cpa))

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

                #put the item into db
                try:
                    response = keyword_table.put_item(
                        Item={
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
                            'avg_cpt': avg_cpt
                        }
                    )
                except ClientError as e:
                    logger.info("runAppleIntegrationKeyword:::process:::PutItem failed due to" + e.response['Error']['Message'])
                else:
                    logger.debug("runAppleIntegrationKeyword:::process:::PutItem succeeded:")

    return True


def export_dict_to_csv(raw_dict, filename):
    df = pd.DataFrame.from_dict(raw_dict)
    df.to_csv(filename, index=None)

def process():
    keyword_table = dynamodb.Table('apple_keyword')
    # To output the keyword_table use the following command. For QC only.
    # export_dict_to_csv(keyword_table.scan()["Items"], "./apple_keyword.txt")

    for client in clientsG:
        print("runAppleIntegrationKeyword:::" + client.clientName + ":::" + str(client.orgId))
        campaignIds = client.campaignIds
        for campaignId in campaignIds:
                # TODO JF maybe should implement a max date call, but pull ONE value, sorted and read max date
                # date_results = keyword_table.scan(FilterExpression=Key('campaignId').eq(str(campaignId)))
                start_date = datetime.date.today() - datetime.timedelta(days=LOOKBACK)
                end_date = datetime.date.today()
                print("start_date:::" + str(start_date))
                print("end_date::: " + str(end_date))
                data = getKeywordReportFromApple(client, campaignId, start_date, end_date)
                
                # load to Dynamo
                if (data is not None) and (data != 'false'):
                    loaded = loadAppleKeywordToDynamo(data, keyword_table)
                else:
                    print("runAppleIntegrationKeyword:::no data returned")

def terminate():
    pass

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
        'body': json.dumps('Run Apple Integration Keyword Complete')
    }
