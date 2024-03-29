
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
import traceback
import sys
from configuration import config
from utils.debug import debug, dprint
from utils.retry import retry
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
from Client import Client

#SK: TODO Configurable variable to go in clients.json
cpi_bid_adjustment_poor_performer = 0.1
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

# FOR QA PURPOSES set these fields explicitly
#start_date = dt.strptime('2019-12-01', '%Y-%m-%d').date()
#end_date = dt.strptime('2019-12-08', '%Y-%m-%d').date()

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
    logger = LambdaUtils.getLogger(clientEvent['rootEvent']['env'])  
    logger.info("runAdGroupBidAdjusterPoorPerformer:::initialize(), rootEvent='" + str(clientEvent['rootEvent']))


@retry
def getAdgroupReportFromAppleHelper(url, cert, json, headers):
  return requests.post(
    url, 
    cert=cert, 
    json=json, 
    headers=headers, 
    timeout=config.HTTP_REQUEST_TIMEOUT
  )

@retry
def getAdgroupReportByTokenHelper(url, json, headers):
  return requests.post(
    url, 
    json=json, 
    headers=headers, 
    timeout=config.HTTP_REQUEST_TIMEOUT
  )

def getAdgroupReportFromApple(campaign):
  payload = { 
    "startTime": str(start_date), 
    "endTime": str(end_date),
    #"granularity": 2, # 1=hourly, 2=daily, 3=monthly, etc.
    "selector": {
      "orderBy": [
        {
          "field":"localSpend",
          "sortOrder" : "DESCENDING"
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
        "offset":0,
        "limit":1000
      }
    },
    #"groupBy":["COUNTRY_CODE"], 
    "returnRowTotals": True, 
    "returnRecordsWithNoMetrics": True
  }


  response = dict()
  # NOTE pivot on token until v3 sunset

  if authToken is not None:
    url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_ADGROUP_REPORTING_URL_TEMPLATE % campaign['campaignId']
    headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % clientG.orgId}
    dprint("\nURL is '%s'." % url)
    dprint("\nPayload is '%s'." % payload)
    dprint ("\nHeaders are %s." % headers)
    response = getAdgroupReportByTokenHelper(
      url,
      json=payload,
      headers=headers
    )
  else:
    url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_ADGROUP_REPORTING_URL_TEMPLATE % campaign['campaignId']
    headers = { "Authorization": "orgId=%s" % clientG.orgId }
    dprint("\nURL is '%s'." % url)
    dprint("\nPayload is '%s'." % payload)
    dprint ("\nHeaders are %s." % headers)
    response = getAdgroupReportFromAppleHelper(
      url,
      cert=(S3Utils.getCert(clientG.pemFilename), S3Utils.getCert(clientG.keyFilename)),
      json=payload,
      headers=headers
    )

  dprint ("\nResponse is %s." % response)

  if response.status_code != 200:
    email = "client id:%d \n url:%s \n payload:%s \n response:%s" % (clientG.orgId, url, payload, response)
    date = time.strftime("%m/%d/%Y")
    subject ="%s - %d ERROR in runAdGroupBidAdjusterPoorPerformer for %s" % (date, response.status_code, clientG.clientName)
    logger.warn(email)
    logger.error(subject)
    if sendG:
      EmailUtils.sendTextEmail(email, subject, emailToG, [], config.EMAIL_FROM)
        
    return False

  return json.loads(response.text)

def createUpdatedAdGroupBids(data, campaign):
  rows = data["data"]["reportingDataResponse"]["row"]
  
  if len(rows) == 0:
    return False

  ABP = clientG.adgroupBidParameters
  dprint("Using adgroup bid parameters %s." % ABP)
  HIGH_CPI_BID_DECREASE_THRESH = ABP["HIGH_CPI_BID_DECREASE_THRESH"]

  # compile data from json library and put into dataframe
  adGroup_info = defaultdict(list)
  for row in rows:
      adGroup_info['adGroupName']            .append(row['metadata']['adGroupName'])
      adGroup_info['adGroupId']              .append(row['metadata']['adGroupId'])
      adGroup_info['bid']                    .append(row['metadata']['defaultBidAmount']['amount'])
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
  adGroup_info["bid_multiplier"] = HIGH_CPI_BID_DECREASE_THRESH / adGroup_info["avgCPA"]

  for index, row in adGroup_info.iterrows():
    # print("-----------------------------------------------------------------------------------")
    # print("bid: " + str(row['bid']))
    # print("taps: " + str(row['taps']))
    # print("TAP_THRESHOLD: " + str(ABP["TAP_THRESHOLD"]))
    # print("installs: " + str(row['installs']))
    # print("NO_INSTALL_BID_DECREASE_THRESH: " + str(ABP["NO_INSTALL_BID_DECREASE_THRESH"]))
    # print("avgCPA: " + str(row['avgCPA']))
    # print("high CPI bid decrease thresh * 2: " + str(HIGH_CPI_BID_DECREASE_THRESH * 2))

    if (row['avgCPA'] > (HIGH_CPI_BID_DECREASE_THRESH * 2) and row['taps'] >= ABP["TAP_THRESHOLD"]) or (row['installs'] <= ABP["NO_INSTALL_BID_DECREASE_THRESH"] and (row['taps'] >= ABP["TAP_THRESHOLD"])):
      newValue = (row['bid'] * .9)
      adGroup_info.at[index,'bid'] = newValue
      logger.info('runAdgroupBidAdjusterPoorPerformer.py:::LOWERING ADGROUP BID TO' + str(newValue))

  #include campaign id info per apple search ads requirement
  adGroup_info['campaignId'] = adGroup_info.shape[0]*[campaign['campaignId']]
  
  #extract only the columns you need per apple search ads requirement
  adGroup_info = adGroup_info[
    [
      'adGroupId',
      'campaignId',
      'adGroupName',
      'bid'
      ]
    ]
  
  #round the values by two
  adGroup_info = np.round(adGroup_info, decimals=2)
  
  #update column names per apple search ads requirement
  adGroup_info.columns = [
    'id',
    'campaignId',
    'name',
    'defaultBidAmount'
  ]
  
  #convert dataframe back to json file for updating
  adGroup_file_to_post = adGroup_info.to_json(orient = 'records')
  result = json.loads(adGroup_file_to_post)
  return result, len(result)

# @retry
def sendOneUpdatedBidToAppleHelper(url, cert, json, headers):
  return requests.put(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

def sendOneUpdatedBidByTokenHelper(url, json, headers):
  return requests.put(url, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

@debug
def sendOneUpdatedBidToApple(adGroup, headers, currency):
  campaignId, adGroupId, bid = adGroup["campaignId"], adGroup["id"], adGroup["defaultBidAmount"]
  del adGroup["campaignId"]
  del adGroup["id"]
  del adGroup["defaultBidAmount"]
  adGroup["defaultBidAmount"] = {"amount": "%.2f" % bid, "currency": currency}

  url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_ADGROUP_UPDATE_URL_TEMPLATE % (campaignId, adGroupId)
  dprint ("URL is '%s'." % url)
  dprint ("Payload is '%s'." % adGroup)

  if (len(adGroup) == 0):
    print("No adGroup data. NOT actually sending anything to apple.")
    return False

  if not sendG:
    print("NOT actually sending anything to apple.")
    return False

  if sendG:
    if authToken is not None:
      response = sendOneUpdatedBidByTokenHelper(
        url,
        json=adGroup,
        headers=headers
      )
    else:
      response = sendOneUpdatedBidToAppleHelper(
        url,
        cert=(S3Utils.getCert(clientG.pemFilename), S3Utils.getCert(clientG.keyFilename)),
        json=adGroup,
        headers=headers
      )

    if response.status_code != 200:
      email = "client id:%d \n url:%s \n response:%s" % (clientG.orgId, url, response)
      date = time.strftime("%m/%d/%Y")
      subject ="%s:%d ERROR in runAdGroupBidAdjusterPoorPerformer for %s" % (date, response.status_code, clientG.clientName)
      logger.warn(email)
      logger.error(subject)
      if sendG:
        EmailUtils.sendTextEmail(email, subject, emailToG, [], config.EMAIL_FROM)
        
    print("The result of sending the update to Apple: %s" % response)   
  return sendG

@debug
def sendUpdatedBidsToApple(adGroupFileToPost):
  # The adGroupFileToPost payload looks like this:
  #  [
  #    { "id"            : 158698070, # That's the adgroup ID.
  #      "campaign_id"   : 158675458,
  #      "name"          : "exact_match",
  #      "defaultBidAmount" : 0.28
  #    }
  #  ]
  #

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
      "Content-Type" : "application/json",
      "Accept"       : "application/json",
    }

  dprint ("Headers are %s." % headers)
  dprint ("PEM='%s'." % clientG.pemFilename)
  dprint ("KEY='%s'." % clientG.keyFilename)

  results = [sendOneUpdatedBidToApple(item, headers, clientG.currency) for item in adGroupFileToPost]
  return True in results # Convert the vector into a scalar.

def createEmailBody(data, sent):
  content = ["""Sent to Apple is %s.""" % sent,
             """\t""".join(["Client", "Campaign", "Updated Ad Group Bids"])]

  for client, clientData in data.items():
    content.append(client)
    for campaignId, payload in clientData.items():
      content.append("""\t%s\t%s""" % (campaignId, payload))

  return "\n".join(content)


def emailSummaryReport(data, sent):
    messageString = createEmailBody(data, sent)
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = "%s - Ad Group Bid Adjuster Poor Performer summary for %s" % (clientG.clientName, dateString)
    EmailUtils.sendTextEmail(messageString, subjectString, emailToG, [], config.EMAIL_FROM)


def process():
  print("runAdgroupBidAdjusterPoorPerformer:::" + clientG.clientName + ":::" + str(clientG.orgId))
  summaryReportInfo = { }
  sent = False
  summaryReportInfo["%s (%s)" % (clientG.orgId, clientG.clientName)] = clientSummaryReportInfo = { }
  
  appleCampaigns = clientG.appleCampaigns
  campaignsForAdgroupBidAdjuster = list(
    filter(
      lambda campaign:(campaign["adgroupBidAdjusterEnabled"] == True), appleCampaigns
    )
  )

  for campaign in campaignsForAdgroupBidAdjuster:

    logger.info("running for campaign type " + campaign['campaignType'])

    data = getAdgroupReportFromApple(campaign)
    if not data:
      logger.info("runAdgroupBidAdjusterPoorPerformer:process:::no results from api:::")
      return

    stuff = createUpdatedAdGroupBids(data, campaign)
    if type(stuff) != bool:
      updatedBids, numberOfBids = stuff
      logger.info("runAdgroupBidAdjusterPoorPerformer: updatedBids " + str(updatedBids))
      logger.info("runAdgroupBidAdjusterPoorPerformer: numberOfBids " + str(numberOfBids))
      sent = sendUpdatedBidsToApple(updatedBids)
      clientSummaryReportInfo[campaign["campaignId"]] = json.dumps(updatedBids)
      clientG.writeUpdatedAdgroupBids(dynamodb, numberOfBids)

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
    except Exception as e: 
        return {
            'statusCode': 400,
            'body': json.dumps('Run Adgroup Poor Performer Failed: ' + str(traceback.format_exception(*sys.exc_info())))
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Adgroup Poor Performer Complete for ' + clientG.clientName)
    }