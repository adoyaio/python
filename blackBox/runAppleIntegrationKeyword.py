#! /usr/bin/python3
import logging
import decimal
from decimal import *
from collections import defaultdict
from datetime import datetime as dt
import datetime
import json
import numpy as np
import pandas as pd
import pprint
import requests
import time
import boto3
from boto3.dynamodb.conditions import Key

# TODO this was to eliminate the inexact and rounding errors
from boto3.dynamodb.types import DYNAMODB_CONTEXT

# Inhibit Inexact Exceptions
from botocore.exceptions import ClientError

DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

from datetime import date

from utils import EmailUtils, DynamoUtils
# from Client import CLIENTS
from configuration import EMAIL_FROM, \
    APPLE_KEYWORD_REPORTING_URL_TEMPLATE, \
    APPLE_ADGROUP_UPDATE_URL_TEMPLATE, \
    TOTAL_COST_PER_INSTALL_LOOKBACK, \
    HTTP_REQUEST_TIMEOUT

from debug import debug, dprint
from retry import retry

BIDDING_LOOKBACK = 7  # days
sendG = False  # Set to True to enable sending data to Apple, else a test run.
logger = logging.getLogger()


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


#@debug
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
        logger.setLevel(logging.INFO)  # TODO reduce AWS logging in production
        # debug.disableDebug() TODO disable debug wrappers in production
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)

    clientsG = DynamoUtils.getClients(dynamodb)
    logger.info("In runAppleIntegrationKeyword:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


# ------------------------------------------------------------------------------
@retry
def getKeywordReportFromAppleHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)


# ------------------------------------------------------------------------------
#@debug
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

    url = APPLE_KEYWORD_REPORTING_URL_TEMPLATE % campaign_id
    print(url)

    headers = {"Authorization": "orgId=%s" % client.orgId}

    dprint("URL is '%s'." % url)
    dprint("Payload is '%s'." % payload)
    dprint("Headers are %s." % headers)

    response = getKeywordReportFromAppleHelper(url,
                                               cert=(client.pemPathname, client.keyPathname),
                                               json=payload,
                                               headers=headers)

    print("runAppleIntegrationKeyword:::Response is " + str(response))

    if response.status_code == 200:
        return json.loads(response.text, parse_float=decimal.Decimal)
    else:
        return 'false'

def calc_date_range_list(start_date, end_date, maximum_dates=90):
    '''
    The Apple Keyword API does not allow more than 90 days per request.

    If the date range is longer than 90 days, then this function will create
    groups of date ranges that are 90 days apart. It returns a list of tuples
    with the start and end dates for each grouping of dates.

    Example: start_date='2019-01-01' and end_date='2019-04-30'
    Return List: [('2019-01-01, '2019-03-31'),('2019-04-01', '2019-04-30')]

    '''
    # final list to return
    group_list = []
    # this is a date that will be iterated over until the iter_date is greater than or equal to the end_date
    iter_start = start_date
    iter_end = start_date + datetime.timedelta(days=maximum_dates - 1)

    # Do until the iter_end date is greater or equal to the end_date
    while (end_date - iter_end).days > 0:
        group_list.append((iter_start, iter_end))
        # update start and end dates to iterate over
        iter_start = iter_end + datetime.timedelta(days=1)
        iter_end = iter_start + datetime.timedelta(days=maximum_dates - 1)

    # After the while condition is met, we need to add the final date range that includes the final end date
    group_list.append((iter_start, end_date))

    return group_list


# ------------------------------------------------------------------------------
#@debug
def loadAppleKeywordToDynamo(data, keyword_table):
    """
    This data will take the raw data from the Apple API call and it will load the data to a DynamoDB.

    The name of the DynamoDB table is apple_keyword

    apple_adgroup
    """

    rows = data["data"]["reportingDataResponse"]["row"]
    if len(rows) == 0:
        logger.debug("loadAppleKeywordToDynamo::NO ROWS")
        return False  # EARLY RETURN

    for row in rows:
            logger.debug("loadAppleKeywordToDynamo:::row:::" + str(row))
            if "total" in row.keys():
                field_key = "total"
            else:
                field_key = "granularity"

            logger.debug("loadAppleKeywordToDynamo:::using field key:::" + field_key)
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

                #now put the item into db
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
                    logger.info("runAppleIntegrationKeyword:process:::PutItem failed due to" + e.response['Error']['Message'])
                else:
                    logger.debug("runAppleIntegrationKeyword:process:::PutItem succeeded:")

    return True


def get_max_date(item_list):
    '''
    This function takes a list of items returned from a dynamoDB table, and it returns the max_date from the list.
    Example:
    [
      {'date': '2019-03-04', 'lat_on_installs': Decimal('0')},
      {'date': '2019-03-05', 'lat_on_installs': Decimal('0')}
    ]
    Should return '2019-03-05'
    '''
    # Initialize a max date
    max_date = dt.strptime("2000-01-01", "%Y-%m-%d").date()

    for elmt in item_list:
        elmt_date = dt.strptime(elmt["date"], "%Y-%m-%d").date()
        print(elmt_date)
        if elmt_date > max_date:
            max_date = elmt_date

    return max_date


def export_dict_to_csv(raw_dict, filename):
    '''
    This function takes a json and a filename, and it exports the json as a csv to the given filename.
    '''
    df = pd.DataFrame.from_dict(raw_dict)
    df.to_csv(filename, index=None)


# ------------------------------------------------------------------------------
# @debug
def process():
    # This first for loop is to load all the keyword data
    keyword_loading_lookback = 14
    keyword_table = dynamodb.Table('apple_keyword')

    # To output the keyword_table use the following command. For QC only.
    # export_dict_to_csv(keyword_table.scan()["Items"], "./apple_keyword.txt")
    # input()

    #for client in CLIENTS:
    for client in clientsG:
        print("Loading Keyword Data for: " + str(client.clientName))
        print(client.orgId)

        campaign_keys = client.keywordAdderIds["campaignId"].keys()

        for campaign_key in campaign_keys:
            print("campaign_key in " + str(campaign_key))

            for campaign_id in [client.keywordAdderIds["campaignId"][campaign_key]]:  # iterate all campaigns
                logger.debug("campaign_id in " + str(campaign_id))
                date_results = keyword_table.scan(FilterExpression=Key('campaign_id').eq(str(campaign_id)))
                logger.debug("date results:::" + str(len(date_results["Items"])))
                logger.debug("date results:::" + str(date_results["Count"]))

                if len(date_results["Items"]) == 0:
                    start_date = datetime.date.today() - datetime.timedelta(days=keyword_loading_lookback)
                    end_date = datetime.date.today()
                else:
                    # Get the start date from the maximum date in the table
                    start_date = get_max_date(date_results["Items"])
                    end_date = datetime.date.today()

                print("START_DATE::: " + str(start_date))
                print("END_DATE::: " + str(end_date))

                # if the start date matches 2000-01-01, then none of the values in the able were later than that date
                # TODO this might be a bad implementation
                if start_date == dt.strptime("2000-01-01", "%Y-%m-%d").date():
                    print("There was an error with getting the maximum date")
                    break

                # if the start_date and the end_date are equal, then the table is up to date
                elif start_date == end_date:
                    print("The apple_keyword table are up to date for {}".format(str(campaign_id)))
                    break

                data = getKeywordReportFromApple(client, campaign_id, start_date, end_date)
                # load the data into Dynamo
                if (data is not None) and (data != 'false'):
                    loaded = loadAppleKeywordToDynamo(data, keyword_table)
                else:
                    print("There was no data returned.")

# ------------------------------------------------------------------------------
#@debug
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
        'body': json.dumps('Run Apple Integration Keyword Complete')
    }
