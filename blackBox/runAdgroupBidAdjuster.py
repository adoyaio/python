
import datetime
import decimal
import json
import logging
import time
from collections import defaultdict
import boto3
import numpy as np
import pandas as pd
import requests
from configuration import config
from utils.debug import debug, dprint
from utils.retry import retry
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
from Client import Client

BIDDING_LOOKBACK = 14

date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta

# FOR QA PURPOSES set these fields explicitly
#start_date = dt.strptime('2019-12-01', '%Y-%m-%d').date()
#end_date = dt.strptime('2019-12-08', '%Y-%m-%d').date()

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
    logger.info("In runAdgroupBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


@retry
def getAdgroupReportFromAppleHelper(url, cert, json, headers):
  return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)


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
                                                       'installs': 0,
                                                       'latOffInstalls': 0,
                                                       'latOnInstalls': 0,
                                                       'newDownloads': 0,
                                                       'redownloads': 0,
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
                                                                 "installs",
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
  url = config.APPLE_ADGROUP_REPORTING_URL_TEMPLATE % client.keywordAdderIds["campaignId"]["search"];

  headers = { "Authorization": "orgId=%s" % client.orgId }
  dprint ("\n\nURL is '%s'." % url)
  dprint ("\n\nPayload is '%s'." % payload)
  dprint ("\n\nHeaders are %s." % headers)
  response = getAdgroupReportFromAppleHelper(url,
                                             cert=(S3Utils.getCert(client.pemFilename),
                                                   S3Utils.getCert(client.keyFilename)),
                                             json=payload,
                                             headers=headers)
  dprint ("\n\nResponse is %s." % response)
  return json.loads(response.text) 


def createUpdatedAdGroupBids(data, client):
  rows = data["data"]["reportingDataResponse"]["row"]
  
  if len(rows) == 0:
    return False

  # NOTE using bidParams vs adgroupBidParameters e.g ABP = client.adgroupBidParameters
  ABP = client.bidParameters
  dprint("Using adgroup bid parameters %s." % ABP)

  # compile data from json library and put into dataframe
  adGroup_info = defaultdict(list)
  for row in rows:
      adGroup_info['adGroupName']            .append(row['metadata']['adGroupName'])
      adGroup_info['adGroupId']              .append(row['metadata']['adGroupId'])
      adGroup_info['bid']                    .append(row['metadata']['defaultCpcBid']['amount'])
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
      adGroup_info['adGroupServingStatus']   .append(row['metadata']['adGroupServingStatus'])
  
  #convert to dataframe    
  adGroup_info = pd.DataFrame(adGroup_info)
  
  #pull in active ad groups only
  adGroup_info = adGroup_info[adGroup_info['adGroupServingStatus'] == 'RUNNING']
  
  #extract only the columns you need for keyword bids
  adGroup_info = adGroup_info[['adGroupName', \
                               'adGroupId', \
                               'impressions', \
                               'taps', \
                               'installs', \
                               'avgCPA', \
                               'localSpend',
                               'bid']]    
  
  #first convert avg cpa to float so you can perform calculations
  adGroup_info['impressions'] = adGroup_info['impressions'].astype(float)
  adGroup_info['taps']        = adGroup_info['taps'].astype(float)
  adGroup_info['installs']    = adGroup_info['installs'].astype(float)
  adGroup_info['avgCPA']      = adGroup_info['avgCPA'].astype(float)
  adGroup_info['localSpend']  = adGroup_info['localSpend'].astype(float)
  adGroup_info['bid']         = adGroup_info['bid'].astype(float)

  # calculate bid multiplier and create a new column
  adGroup_info["bid_multiplier"] = ABP["HIGH_CPI_BID_DECREASE_THRESH"] / adGroup_info["avgCPA"]

  # cap bid multiplier
  if ABP["OBJECTIVE"] == "aggressive":
      adGroup_info['bid_multiplier_capped'] = np.clip(adGroup_info['bid_multiplier'], 0.90, 1.30)
  elif ABP["OBJECTIVE"] == "standard":
      adGroup_info['bid_multiplier_capped'] = np.clip(adGroup_info['bid_multiplier'], 0.80, 1.20)
  elif ABP["OBJECTIVE"] == "conservative":
      adGroup_info['bid_multiplier_capped'] = np.clip(adGroup_info['bid_multiplier'], 0.70, 1.10)
  else:
      print("no objective selected")

  # create upper bid cap tied to target cost per install.
  if ABP["OBJECTIVE"] == "aggressive":
      bidCap_targetCPI = ABP["HIGH_CPI_BID_DECREASE_THRESH"] * 1.20
  elif ABP["OBJECTIVE"] == "standard":
      bidCap_targetCPI = ABP["HIGH_CPI_BID_DECREASE_THRESH"] * 1
  elif ABP["OBJECTIVE"] == "conservative":
      bidCap_targetCPI = ABP["HIGH_CPI_BID_DECREASE_THRESH"] * 0.80
  else:
      print("no objective selected")

  #write your conditional statement for the different scenarios
  adGroup_info_cond = [(adGroup_info.avgCPA      <= ABP["HIGH_CPI_BID_DECREASE_THRESH"]) & \
                       (adGroup_info.taps        >= ABP["TAP_THRESHOLD"]) & \
                       (adGroup_info.installs    >  ABP["NO_INSTALL_BID_DECREASE_THRESH"]),
                       (adGroup_info.avgCPA      >  ABP["HIGH_CPI_BID_DECREASE_THRESH"]) & \
                       (adGroup_info.taps        >= ABP["TAP_THRESHOLD"]),
                       (adGroup_info.taps        <  ABP["TAP_THRESHOLD"]),
                       (adGroup_info.installs    == ABP["NO_INSTALL_BID_DECREASE_THRESH"]) & \
                       (adGroup_info.taps        >= ABP["TAP_THRESHOLD"])]

  adGroup_info_choices = [adGroup_info.bid * adGroup_info["bid_multiplier_capped"].round(2),
                          adGroup_info.bid * adGroup_info["bid_multiplier_capped"].round(2),
                          adGroup_info.bid * ABP["STALE_RAISE_BID_BOOST"],
                          adGroup_info.bid * adGroup_info["bid_multiplier_capped"].round(2)]

  adGroup_info_choices_increases = [adGroup_info.bid * adGroup_info["bid_multiplier_capped"].round(2),
                                    adGroup_info.bid * 1,
                                    adGroup_info.bid * ABP["STALE_RAISE_BID_BOOST"],
                                    adGroup_info.bid * 1]

  #check if overall CPI is within bid threshold, if it is, do not decrease bids 
  total_cost_per_install = client.getTotalCostPerInstall(dynamodb, start_date, end_date,
                                                         config.TOTAL_COST_PER_INSTALL_LOOKBACK)
  dprint("runAdgroupBidAdjuster:total cpi %s" % str(total_cost_per_install))

  bid_decision = adGroup_info_choices_increases \
                 if total_cost_per_install <= ABP["HIGH_CPI_BID_DECREASE_THRESH"] \
                 else adGroup_info_choices

  #calculate the bid adjustments
  adGroup_info['bid'] = np.select(adGroup_info_cond, bid_decision, default = adGroup_info.taps)

  # enforce min and max bid
  adGroup_info['bid'] = np.clip(adGroup_info['bid'], ABP["MIN_BID"], bidCap_targetCPI)

  #include campaign id info per apple search ads requirement
  adGroup_info['campaignId'] = adGroup_info.shape[0]*[client.keywordAdderIds["campaignId"]["search"]];
  
  #extract only the columns you need per apple search ads requirement
  adGroup_info = adGroup_info[['adGroupId',
                               'campaignId',
                               'adGroupName',
                               'bid']]
  
  #round the values by two
  adGroup_info = np.round(adGroup_info, decimals=2)
  
  #update column names per apple search ads requirement
  adGroup_info.columns = ['id',
                          'campaignId',
                          'name',
                          'defaultCPCBid']
  
  #convert dataframe back to json file for updating
  adGroup_file_to_post = adGroup_info.to_json(orient = 'records')
  result = json.loads(adGroup_file_to_post)
  return result, len(result)

@retry
def sendOneUpdatedBidToAppleHelper(url, cert, json, headers):
  return requests.put(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)


@debug
def sendOneUpdatedBidToApple(client, adGroup, headers, currency):
  campaignId, adGroupId, bid = adGroup["campaignId"], adGroup["id"], adGroup["defaultCPCBid"]
  del adGroup["campaignId"]
  del adGroup["id"]
  del adGroup["defaultCPCBid"]
  adGroup["defaultCpcBid"] = {"amount": "%.2f" % bid, "currency": currency}
  url = config.APPLE_ADGROUP_UPDATE_URL_TEMPLATE % (campaignId, adGroupId)
  dprint ("URL is '%s'." % url)
  dprint ("Payload is '%s'." % adGroup)

  if sendG:
    response = sendOneUpdatedBidToAppleHelper(url,
                                              cert=(S3Utils.getCert(client.pemFilename),
                                                    S3Utils.getCert(client.keyFilename)),
                                              json=adGroup,
                                              headers=headers)
    
  else:
    response = "Not actually sending anything to Apple."
  print ("The result of sending the update to Apple: %s" % response)
  return sendG


@debug
def sendUpdatedBidsToApple(client, adGroupFileToPost):
  # The adGroupFileToPost payload looks like this:
  #  [
  #    { "id"            : 158698070, # That's the adgroup ID.
  #      "campaign_id"   : 158675458,
  #      "name"          : "exact_match",
  #      "defaultCpcBid" : 0.28
  #    }
  #  ]
  #
  # It's an array; can it have more than one entry? Zero entries?

  headers = { "Authorization": "orgId=%s" % client.orgId,
              "Content-Type" : "application/json",
              "Accept"       : "application/json",
            }

  dprint ("Headers are %s." % headers)
  dprint ("PEM='%s'." % client.pemFilename)
  dprint ("KEY='%s'." % client.keyFilename)

  results = [sendOneUpdatedBidToApple(client, item, headers, client.currency) for item in adGroupFileToPost]
  return True in results # Convert the vector into a scalar.

@debug
def createEmailBody(data, sent):
  content = ["""Sent to Apple is %s.""" % sent,
             """\t""".join(["Client", "Campaign", "Updated Ad Group Bids"])]

  for client, clientData in data.items():
    content.append(client)
    for campaignId, payload in clientData.items():
      content.append("""\t%s\t%s""" % (campaignId, payload))

  return "\n".join(content)


@debug
def emailSummaryReport(data, sent):
    messageString = createEmailBody(data, sent);
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = "Ad Group Bid Adjuster summary for %s" % dateString
    EmailUtils.sendTextEmail(messageString, subjectString, EMAIL_TO, [], config.EMAIL_FROM)


@debug
def process():
  summaryReportInfo = { }
  sent = False

  for client in clientsG:
    summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = clientSummaryReportInfo = { }
    campaignIds = client.campaignIds

    for campaignId in campaignIds:
      data = getAdgroupReportFromApple(client)
      stuff = createUpdatedAdGroupBids(data, client)
      if type(stuff) != bool:
        updatedBids, numberOfBids = stuff
        print("runAdgroupBidAdjuster: updatedBids " + str(updatedBids))
        print("runAdgroupBidAdjuster: numberOfBids " + str(numberOfBids))
        sent = sendUpdatedBidsToApple(client, updatedBids)
        #if sent:
          # TODO: Pull just the relevant field (defaultCPCBid?) from updatedBids, not the whole thing. --DS, 31-Dec-2018
        clientSummaryReportInfo[client.keywordAdderIds["campaignId"]["search"]] = json.dumps(updatedBids)
        client.writeUpdatedAdgroupBids(dynamodb, numberOfBids)

  emailSummaryReport(summaryReportInfo, sent)

@debug
def terminate():
  pass


# ------------------------------------------------------------------------------
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
        'body': json.dumps('Run Adgroup Bid Adjuster Complete')
    }