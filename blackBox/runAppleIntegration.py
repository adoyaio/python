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

#TODO this was to eliminate the inexact and rounding errors
from boto3.dynamodb.types import DYNAMODB_CONTEXT
# Inhibit Inexact Exceptions
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

from datetime import date

from utils import EmailUtils
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
#start_date = today - start_date_delta
#end_date = today - end_date_delta

# FOR QA PURPOSES set these fields explicitly
#start_date = dt.strptime('2019-12-01', '%Y-%m-%d').date()
#end_date = dt.strptime('2019-12-08', '%Y-%m-%d').date()

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




@debug
def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global dynamodb
    global EMAIL_TO

    EMAIL_TO = emailToInternal

    if env != "prod":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
        logger.setLevel(logging.INFO)
    else:
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)  # TODO reduce AWS logging in production
        # debug.disableDebug() TODO disable debug wrappers in production

    logger.info("In runAppleIntegration:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))

# ------------------------------------------------------------------------------
@retry
def getAdgroupReportFromAppleHelper(url, cert, json, headers):
  return requests.post(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)



# ------------------------------------------------------------------------------
@debug
def getAdgroupReportFromApple(client, start_date, end_date):
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


  payload = { "startTime"                  : str(start_date), 
              "endTime"                    : str(end_date),
              "granularity"                : "DAILY",
              "selector"                   : { "orderBy"    : [ { "field"     : "localSpend",
                                                                  "sortOrder" : "DESCENDING"
                                                                } ], 
                                               "fields"     :  ["localSpend",
                                                                 "taps",
                                                                 "impressions", #TODO is this right?
                                                                 "newDownloads",
                                                                 "redownloads",
                                                                 "latOnInstalls",
                                                                 "latOffInstalls",
                                                                 "avgCPA",
                                                                 "avgCPT",
                                                                 "ttr",
                                                                 "conversionRate"
                                                               ],
                                               "pagination" : { "offset" : 0,
                                                                "limit"  : 1000
                                                              }
                                             },
              "returnRowTotals"            : False,
              "returnRecordsWithNoMetrics" : True
            }
  
  
  url = APPLE_ADGROUP_REPORTING_URL_TEMPLATE % client.keywordAdderIds["campaignId"]["search"]
  print(url)

  headers = { "Authorization": "orgId=%s" % client.orgId }

  dprint ("URL is '%s'." % url)
  dprint ("Payload is '%s'." % payload)
  dprint ("Headers are %s." % headers)

  response = getAdgroupReportFromAppleHelper(url,
                                             cert=(client.pemPathname, client.keyPathname),
                                             json=payload,
                                             headers=headers)

  logger.debug("Response is " + str(response))

  return json.loads(response.text, parse_float=decimal.Decimal) 


def calc_date_range_list(start_date, end_date, maximum_dates = 90):
  '''
  The Apple Keyword API does not allow more than 90 days per request.

  If the date range is longer than 90 days, then this function will create 
  groups of date ranges that are 90 days apart. It returns a list of tuples
  with the start and end dates for each grouping of dates.

  Example: start_date='2019-01-01' and end_date='2019-04-30'
  Return List: [('2019-01-01, '2019-03-31'),('2019-04-01', '2019-04-30')]

  '''
  #final list to return
  group_list=[]
  #this is a date that will be iterated over until the iter_date is greater than or equal to the end_date
  iter_start = start_date
  iter_end = start_date + datetime.timedelta(days=maximum_dates - 1)

  #Do until the iter_end date is greater or equal to the end_date
  while (end_date - iter_end).days > 0:
    group_list.append((iter_start,iter_end))
    #update start and end dates to iterate over
    iter_start = iter_end + datetime.timedelta(days=1)
    iter_end = iter_start + datetime.timedelta(days=maximum_dates - 1)

  #After the while condition is met, we need to add the final date range that includes the final end date
  group_list.append((iter_start, end_date))

  return group_list
  
# ------------------------------------------------------------------------------
@debug
def loadAppleAdGroupToDynamo(data, client, start_date, end_date, adgroup_table):
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


  #If there aren't any daily values, then all the fields will be totaled. Otherwise, the daily values will be in the 
  #"granularity" fields.
  if "total" in rows[0].keys():
    field_key = "total"
  else:
    field_key = "granularity"

  print("using field key: " + field_key)

  #compile data from json library and put into dataframe
  adGroup_info = defaultdict(list)
  for i in range(len(rows[0]['granularity'])):
      #Get the daily values
      #TODO if there is only 1 date, then the totals will not be in a list. Instead, they will simply be in a dictionary
      if type(rows[0][field_key]) == dict:
        #put the data into the table
        adgroup_table.put_item(
              Item={
                  #TODO The original date is in format "2019-11-13T00:00:00.000", but the split("T") command is hacky
                  'creation_date': rows[0]['metadata']['startTime'].split("T")[0],
                  'date' : str(rows[0]['granularity'][i]["date"]),
                  #TODO all of these client fields should be read from the configuration script 
                  'app_name': client.appName,
                  'app_id': str(client.appID),
                  'campaign_name': client.campaignName,
                  'campaign_id': str(client.keywordAdderIds["campaignId"]["search"]),
                  'adgroup_name': rows[0]['metadata']['adGroupName'],
                  'adgroup_id': str(rows[0]['metadata']['adGroupId']),
                  'bid': decimal.Decimal(str(rows[0]['metadata']['defaultCpcBid']['amount'])),
                  'deleted': rows[0]['metadata']['deleted'],
                  'modification_time': rows[0]['metadata']['modificationTime'].split("T")[0],
                  'impressions': rows[0][field_key]['impressions'],
                  'taps': rows[0][field_key]['taps'],
                  'conversions': rows[0][field_key]['installs'],
                  'ttr': rows[0][field_key]['ttr'],
                  'installs': rows[0][field_key]['redownloads'] + rows[0][field_key]['newDownloads'],
                  'new_downloads': rows[0][field_key]['newDownloads'],
                  're_downloads': rows[0][field_key]['redownloads'],
                  'lat_on_installs': rows[0][field_key]['latOnInstalls'],
                  'lat_off_installs': rows[0][field_key]['latOffInstalls'],
                  'avg_cpa': decimal.Decimal(str(rows[0][field_key]['avgCPA']['amount'])),
                  'conversion_rate': decimal.Decimal(str(rows[0][field_key]['conversionRate'])),
                  'local_spend': decimal.Decimal(str(rows[0][field_key]['localSpend']['amount'])),
                  'avg_cpt': str(rows[0][field_key]['avgCPT']['amount'])

              }
          )
      else:
        #put the data into the table
        adgroup_table.put_item(
              Item={
                  'creation_date': rows[0]['metadata']['startTime'].split("T")[0],
                  'date' : str(rows[0]['granularity'][i]["date"]),
                  #TODO all of these client fields should be read from the configuration script 
                  'app_name': client.appName,
                  'app_id': str(client.appID),
                  'campaign_name': client.campaignName,
                  'campaign_id': str(client.keywordAdderIds["campaignId"]["search"]),
                  'adgroup_name': rows[0]['metadata']['adGroupName'],
                  'adgroup_id': str(rows[0]['metadata']['adGroupId']),
                  'bid': decimal.Decimal(str(rows[0]['metadata']['defaultCpcBid']['amount'])),
                  'deleted': rows[0]['metadata']['deleted'],
                  'modification_time': rows[0]['metadata']['modificationTime'].split("T")[0],
                  'impressions': rows[0][field_key][i]['impressions'],
                  'taps': rows[0][field_key][i]['taps'],
                  'conversions': rows[0][field_key][i]['installs'],
                  'ttr': rows[0][field_key][i]['ttr'],
                  'installs': rows[0][field_key][i]['redownloads'] + rows[0][field_key][i]['newDownloads'],
                  'new_downloads': rows[0][field_key][i]['newDownloads'],
                  're_downloads': rows[0][field_key][i]['redownloads'],
                  'lat_on_installs': rows[0][field_key][i]['latOnInstalls'],
                  'lat_off_installs': rows[0][field_key][i]['latOffInstalls'],
                  'avg_cpa': decimal.Decimal(str(rows[0][field_key][i]['avgCPA']['amount'])),
                  'conversion_rate': decimal.Decimal(str(rows[0][field_key][i]['conversionRate'])),
                  'local_spend': decimal.Decimal(str(rows[0][field_key][i]['localSpend']['amount'])),
                  'avg_cpt': decimal.Decimal(str(rows[0][field_key][i]['avgCPT']['amount']))

              }
          )
      

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
  #Initialize a max date
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
  df.to_csv(filename, index = None)

# ------------------------------------------------------------------------------
#@debug
def process():

  #This first for loop is to load all the adgroup date
  #TODO We want to go back a year, but Apple is only allowing 90 days
  adgroup_loading_lookback = 365
  adgroup_table = dynamodb.Table('apple_adgroup')


  #TODOTo output the adgroup_table use the following command. For QC only.
  #export_dict_to_csv(adgroup_table.scan()["Items"], "/home/kenny/adoya/python/blackBox/data/dynamo/apple_adgroup_qc.txt")
  #input()

  for client in CLIENTS:
    print("Loading Adgroup Date for: " + str(client.clientName))
    print(client.orgId)
    print(client.appName)
    print(client.appID)
    print(client.campaignName)

    

    for campaign_id in [client.keywordAdderIds["campaignId"]["search"]]:   #TODO iterate all campaigns

      date_results = adgroup_table.scan(FilterExpression=Key('campaign_id').eq(str(campaign_id)))

      print(len(date_results["Items"]))


      if len(date_results["Items"]) == 0:
        start_date = datetime.date.today() - datetime.timedelta(days=adgroup_loading_lookback)
        end_date = datetime.date.today()
      else:
        
        #Get the start date from the maximum date in the table
        start_date = get_max_date(date_results["Items"])
        end_date = datetime.date.today()


        print(start_date)
        print(end_date)


        #if the start date matches 2000-01-01, then none of the values in the able were later than that date 
        #TODO this might be a bad implementation
        if start_date == dt.strptime("2000-01-01", "%Y-%m-%d").date():
          print("There was an error with getting the maximum date")

          break

        #if the start_date and the end_date are equal, then the table is up to date
        elif start_date == end_date:
          print("The apple_adgroup table are up to date for {}".format(str(campaign_id)))

          break


      date_ranges = calc_date_range_list(start_date, end_date, maximum_dates = 90)
      
      #each of the indexes in the ate_ranges list is a tuple with the (start_date, end_date) for each request
      for date_range in date_ranges: 
        # Get the data from Apple

        data = getAdgroupReportFromApple(client, date_range[0], date_range[1])
        #load the data into Dynamo
        if data is not None:
          loaded = loadAppleAdGroupToDynamo(data, client, date_range[0], date_range[1], adgroup_table)
        else:
          print("There was no data returned.")




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
        'body': json.dumps('Run Apple Integration Complete')
    }