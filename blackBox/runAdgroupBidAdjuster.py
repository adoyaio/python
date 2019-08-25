#! /usr/bin/python3

from collections import defaultdict
import datetime
import email.message
from email.headerregistry import Address
import json
import os
import numpy as np
import pandas as pd
import pprint
import requests
import smtplib 
import sys
import time

if sys.platform == "linux":
  os.chdir("/home/scott/ScottKaplan/Adoya")

else:
  PATH_ADDEND = "C:\\Users\A\\Desktop\\apple_search_ads_api\\adGroup_bid_adjuster"
  print("Adding '%s' to sys.path." % PATH_ADDEND);
  sys.path.append(PATH_ADDEND)

from Client import CLIENTS
from configuration import SMTP_HOSTNAME, \
                          SMTP_PORT, \
                          SMTP_USERNAME, \
                          SMTP_PASSWORD, \
                          EMAIL_FROM, \
                          APPLE_ADGROUP_REPORTING_URL_TEMPLATE, \
                          APPLE_ADGROUP_UPDATE_URL_TEMPLATE, \
                          TOTAL_COST_PER_INSTALL_LOOKBACK, \
                          HTTP_REQUEST_TIMEOUT

from debug import debug, dprint
from retry import retry


BIDDING_LOOKBACK = 7 # days

# TODO: Merge these duplicate EMAIL_FROM and EMAIL_TO into a config.py file. --DS, 9-Nov-2018
EMAIL_TO         = (Address("David Schachter", "davidschachter", "gmail.com"),
                    Address("Scott Kaplan",    "scott.kaplan",   "ssjdigital.com"),
                   )

sendG = False # Set to True to enable sending data to Apple, else a test run.

###### date and time parameters for bidding lookback ######
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta



# ------------------------------------------------------------------------------
@debug
def initialize():
  global sendG


  sendG = "-s" in sys.argv or "--send" in sys.argv
  dprint("In initialize(), getcwd()='%s' and sendG=%s." % (os.getcwd(), sendG))



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
def createUpdatedAdGroupBids(data, client):
  rows = data["data"]["reportingDataResponse"]["row"]

  if len(rows) == 0:
    return False # EARLY RETURN
  
  ######make bid adjustments######

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
      adGroup_info['impressions']            .append(row['total']['impressions'])
      adGroup_info['taps']                   .append(row['total']['taps'])
      adGroup_info['ttr']                    .append(row['total']['ttr'])
      adGroup_info['conversions']            .append(row['total']['conversions'])
      adGroup_info['conversionsNewDownloads'].append(row['total']['conversionsNewDownloads'])
      adGroup_info['conversionsRedownloads'] .append(row['total']['conversionsRedownloads'])
      adGroup_info['conversionsLATOn']       .append(row['total']['conversionsLATOn'])
      adGroup_info['conversionsLATOff']      .append(row['total']['conversionsLATOff'])
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
                               'conversions', \
                               'avgCPA', \
                               'localSpend',
                               'bid']]    
  
  #first convert avg cpa to float so you can perform calculations
  adGroup_info['impressions'] = adGroup_info['impressions'].astype(float)
  adGroup_info['taps']        = adGroup_info['taps'].astype(float)
  adGroup_info['conversions'] = adGroup_info['conversions'].astype(float)
  adGroup_info['avgCPA']      = adGroup_info['avgCPA'].astype(float)
  adGroup_info['localSpend']  = adGroup_info['localSpend'].astype(float)
  adGroup_info['bid']         = adGroup_info['bid'].astype(float)
  
  #write your conditional statement for the different scenarios
  adGroup_info_cond = [(adGroup_info.avgCPA      <= ABP["HIGH_CPI_BID_DECREASE_THRESH"]) & \
                       (adGroup_info.taps        >= ABP["TAP_THRESHOLD"]) & \
                       (adGroup_info.conversions >  ABP["NO_INSTALL_BID_DECREASE_THRESH"]),
                       (adGroup_info.avgCPA      >  ABP["HIGH_CPI_BID_DECREASE_THRESH"]) & \
                       (adGroup_info.taps        >= ABP["TAP_THRESHOLD"]),
                       (adGroup_info.taps        <  ABP["TAP_THRESHOLD"]), 
                       (adGroup_info.conversions == ABP["NO_INSTALL_BID_DECREASE_THRESH"]) & \
                       (adGroup_info.taps        >= ABP["TAP_THRESHOLD"])]
  
  adGroup_info_choices = [adGroup_info.bid * ABP["LOW_CPA_BID_BOOST"],
                          adGroup_info.bid * ABP["HIGH_CPA_BID_DECREASE"],
                          adGroup_info.bid * ABP["STALE_RAISE_BID_BOOST"],
                          adGroup_info.bid * ABP["HIGH_CPA_BID_DECREASE"]]
  
  adGroup_info_choices_increases = [adGroup_info.bid * ABP["LOW_CPA_BID_BOOST"], 
                                    adGroup_info.bid * 1, 
                                    adGroup_info.bid * ABP["STALE_RAISE_BID_BOOST"], 
                                    adGroup_info.bid * 1]

  #check if overall CPI is within bid threshold, if it is, do not decrease bids 
  total_cost_per_install = client.getTotalCostPerInstall(TOTAL_COST_PER_INSTALL_LOOKBACK)

  bid_decision = adGroup_info_choices_increases \
                 if total_cost_per_install <= ABP["HIGH_CPI_BID_DECREASE_THRESH"] \
                 else adGroup_info_choices

  #calculate the bid adjustments
  adGroup_info['bid'] = np.select(adGroup_info_cond, bid_decision, default = adGroup_info.taps)
  
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



# ------------------------------------------------------------------------------
@retry
def sendOneUpdatedBidToAppleHelper(url, cert, json, headers):
  return requests.put(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)



# ------------------------------------------------------------------------------
@debug
def sendOneUpdatedBidToApple(client, adGroup, headers):
  campaignId, adGroupId, bid = adGroup["campaignId"], adGroup["id"], adGroup["defaultCPCBid"]

  del adGroup["campaignId"]
  del adGroup["id"]
  del adGroup["defaultCPCBid"]
  adGroup["defaultCpcBid"] = {"amount": "%.2f" % bid, "currency": "USD"}

  url = APPLE_ADGROUP_UPDATE_URL_TEMPLATE % (campaignId, adGroupId)
  dprint ("URL is '%s'." % url)
  dprint ("Payload is '%s'." % adGroup)

  if sendG:
    response = sendOneUpdatedBidToAppleHelper(url,
                                              cert=(client.pemPathname, client.keyPathname),
                                              json=adGroup,
                                              headers=headers)
    
  else:
    response = "Not actually sending anything to Apple."

  print ("The result of sending the update to Apple: %s" % response)

  return sendG



# ------------------------------------------------------------------------------
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
  dprint ("PEM='%s'." % client.pemPathname)
  dprint ("KEY='%s'." % client.keyPathname)

  results = [sendOneUpdatedBidToApple(client, item, headers) for item in adGroupFileToPost]

  return True in results # Convert the vector into a scalar.



# ------------------------------------------------------------------------------
@debug
def createEmailBody(data, sent):
  content = ["""Sent to Apple is %s.""" % sent,
             """\t""".join(["Client", "Campaign", "Updated Ad Group Bids"])]

  for client, clientData in data.items():
    content.append(client)
    for campaignId, payload in clientData.items():
      content.append("""\t%s\t%s""" % (campaignId, payload))

  return "\n".join(content)



# ------------------------------------------------------------------------------
@debug
def emailSummaryReport(data, sent):
  msg = email.message.EmailMessage()
  msg.set_content(createEmailBody(data, sent))

  dateString = time.strftime("%m/%d/%Y")
  if dateString.startswith("0"):
    dateString = dateString[1:]

  msg['Subject'] = "Ad Group Bid Adjuster summary for %s" % dateString
  msg['From']    = EMAIL_FROM
  msg['To']      = EMAIL_TO
#  msg.replace_header("Content-Type", "text/html")


  # TODO: Merge this duplicate code with runClientDailyReports.py. --DS, 30-Aug-2018
  if sys.platform == "linux": # Don't try to send email on Scott's "Windows" box.
    dprint("SMTP hostname/port=%s/%s" % (SMTP_HOSTNAME, SMTP_PORT))

    with smtplib.SMTP(host=SMTP_HOSTNAME, port=SMTP_PORT) as smtpServer:
      smtpServer.set_debuglevel(2)
      smtpServer.starttls()
      smtpServer.login(SMTP_USERNAME, SMTP_PASSWORD)
      smtpServer.send_message(msg)



# ------------------------------------------------------------------------------
@debug
def process():
  summaryReportInfo = { }
  sent = False

  for client in CLIENTS:
    summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = clientSummaryReportInfo = { }
    campaignIds = client.campaignIds

    for campaignId in campaignIds:
      data = getAdgroupReportFromApple(client)

      stuff = createUpdatedAdGroupBids(data, client)

      if type(stuff) != bool:
        updatedBids, numberOfBids = stuff
        sent = sendUpdatedBidsToApple(client, updatedBids)
        if sent:
          # TODO: Pull just the relevant field (defaultCPCBid?) from updatedBids, not the whole thing. --DS, 31-Dec-2018
          clientSummaryReportInfo[client.keywordAdderIds["campaignId"]["search"]] = json.dumps(updatedBids)
          client.updatedAdgroupBids = numberOfBids

  emailSummaryReport(summaryReportInfo, sent)



# ------------------------------------------------------------------------------
@debug
def terminate():
  pass



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  initialize()
  process()
  terminate()
