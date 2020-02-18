#! /usr/bin/python3
import logging
import decimal
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

from utils import AdoyaEmail
from Client import CLIENTS
from configuration import EMAIL_FROM, \
                          APPLE_ADGROUP_REPORTING_URL_TEMPLATE, \
                          APPLE_ADGROUP_UPDATE_URL_TEMPLATE, \
                          TOTAL_COST_PER_INSTALL_LOOKBACK, \
                          HTTP_REQUEST_TIMEOUT

from debug import debug, dprint
from retry import retry

BIDDING_LOOKBACK = 7 # days
sendG = False # Set to True to enable sending data to Apple, else a test run.

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

    logger.info("In runAdgroupBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))

# ------------------------------------------------------------------------------
@retry
def getAdgroupReportFromAppleHelper(url, cert, json, headers):
  return requests.post(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)



# ------------------------------------------------------------------------------
@debug
def getAdgroupReportFromApple(client):
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
                                                       'conversions': 0,
                                                       'conversionsLATOff': 0,
                                                       'conversionsLATOn': 0,
                                                       'conversionsNewDownloads': 0,
                                                       'conversionsRedownloads': 0,
                                                       'impressions': 0,
                                                       'localSpend': {'amount': '0',
                                                                      'currency': 'USD'},
                                                       'taps': 0,
                                                       'ttr': 0.0}}]}},
  'error': None,
  'pagination': {'itemsPerPage': 1, 'startIndex': 0, 'totalResults': 1}}
  """

  payload = { "startTime"                  : str(start_date), 
              "endTime"                    : str(end_date),
              #"granularity"                : 2, # 1=hourly, 2=daily, 3=monthly, etc.
              "selector"                   : { "orderBy"    : [ { "field"     : "localSpend",
                                                                  "sortOrder" : "DESCENDING"
                                                                } ], 
                                               "fields"     :  [ "localSpend",
                                                                 "taps",
                                                                 "impressions",
                                                                 "conversions",
                                                                 "avgCPA",
                                                                 "avgCPT",
                                                                 "ttr",
                                                                 "conversionRate"
                                                               ],
                                               "pagination" : { "offset" : 0,
                                                                "limit"  : 1000
                                                              }
                                             },
              #"groupBy"                    : ["COUNTRY_CODE"], 
              "returnRowTotals"            : True, 
              "returnRecordsWithNoMetrics" : True
            }
  url = APPLE_ADGROUP_REPORTING_URL_TEMPLATE % client.keywordAdderIds["campaignId"]["search"];

  headers = { "Authorization": "orgId=%s" % client.orgId }

  dprint ("URL is '%s'." % url)
  dprint ("Payload is '%s'." % payload)
  dprint ("Headers are %s." % headers)

  response = getAdgroupReportFromAppleHelper(url,
                                             cert=(client.pemPathname, client.keyPathname),
                                             json=payload,
                                             headers=headers)
  dprint ("Response is %s." % response)

  return json.loads(response.text) 




# ------------------------------------------------------------------------------
@debug
def loadAppleAdGroupToDynamo(data, client):
  '''
  This data will take the raw data from the Apple API call and it will load the data to a DynamoDB.

  The name of the DynamoDB table is apple_adGroup


  apple_adgroup
  '''
  rows = data["data"]["reportingDataResponse"]["row"]

  if len(rows) == 0:
    return False # EARLY RETURN
  
  # In email of 28-Feb-2019, Scott asked use to use bidParameters, not adGroupBidParameters. --DS
  #ABP = client.adgroupBidParameters;
  ABP = client.bidParameters;

  dprint("Using adgroup bid parameters %s." % ABP)

  #compile data from json library and put into dataframe
  adGroup_info = defaultdict(list)
  for row in rows:
      adGroup_info['adGroupName']            .append(row['metadata']['adGroupName'])
      adGroup_info['adGroupId']              .append(row['metadata']['adGroupId'])
      adGroup_info['bid']                    .append(row['metadata']['defaultCpcBid']['amount'])
      adGroup_info['deleted']                .append(row['metadata']['deleted'])
      adGroup_info['modificationTime']       .append(row['metadata']['modificationTime'])
      adGroup_info['impressions']            .append(row['total']['impressions'])
      adGroup_info['taps']                   .append(row['total']['taps'])
      adGroup_info['ttr']                    .append(row['total']['ttr'])
      adGroup_info['installs']               .append(row['total']['installs'])
      adGroup_info['newDownloads']           .append(row['total']['newDownloads'])
      adGroup_info['redownloads']            .append(row['total']['redownloads'])
      adGroup_info['latOnInstalls']          .append(row['total']['latOnInstalls'])
      adGroup_info['latOffInstalls']         .append(row['total']['latOffInstalls'])     
      adGroup_info['avgCPA']                 .append(row['total']['avgCPA']['amount'])
      adGroup_info['conversionRate']         .append(row['total']['conversionRate'])
      adGroup_info['localSpend']             .append(row['total']['localSpend']['amount'])	
      adGroup_info['avgCPT']                 .append(row['total']['avgCPT']['amount'])




  #convert to dataframe    
  adGroup_info = pd.DataFrame(adGroup_info)
  
  return True




# ------------------------------------------------------------------------------
@debug
def process():
  summaryReportInfo = { }
  sent = False

  for client in CLIENTS:
    print(client.orgId)
    print(client.clientName)
    campaignIds = client.campaignIds
    print(campaignIds)
    for campaignId in campaignIds:
      data = getAdgroupReportFromApple(client)

      input()
      '''
      stuff = createUpdatedAdGroupBids(data, client)

      print("runAdgroupBidAdjuster: stuff " + str(stuff))
      if type(stuff) != bool:
        updatedBids, numberOfBids = stuff
        print("runAdgroupBidAdjuster: updatedBids " + str(updatedBids))
        print("runAdgroupBidAdjuster: numberOfBids " + str(numberOfBids))
        sent = sendUpdatedBidsToApple(client, updatedBids)
        #if sent:
          # TODO: Pull just the relevant field (defaultCPCBid?) from updatedBids, not the whole thing. --DS, 31-Dec-2018
        clientSummaryReportInfo[client.keywordAdderIds["campaignId"]["search"]] = json.dumps(updatedBids)
        client.updatedAdgroupBids(dynamodb, numberOfBids)
      '''
  #emailSummaryReport(summaryReportInfo, sent)



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
        'body': json.dumps('Run Adgroup Bid Adjuster Complete')
    }