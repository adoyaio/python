#! /usr/bin/python3
import logging
import datetime
import email.message
from email.headerregistry import Address
import json
import os
import pprint
import requests
import smtplib
import sys
import time
from utils import AdoyaEmail
import boto3
from Client import CLIENTS
from configuration import EMAIL_FROM, \
    APPLE_KEYWORDS_REPORT_URL, \
    HTTP_REQUEST_TIMEOUT

from debug import debug, dprint
from retry import retry

ONE_DAY = 1
SEVEN_DAYS = 7
FOUR_YEARS = 365 * 4  # Ignoring leap years.
EMAIL_SUBJECT = """%s - Apple Search Ads Update %s"""

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sendG = False  # Set to True to enable sending email to clients, else a test run.

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

    logger.info("In runClientDailyReport:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


# ------------------------------------------------------------------------------
@retry
def getCampaignDataHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)


# ------------------------------------------------------------------------------
@debug
def getCampaignData(orgId, pemPathname, keyPathname, daysToGoBack):
    ######enter date and time parameters for bidding lookback######
    # Subtract 1 day because the program runs at 3 am the next day.
    today = datetime.date.today() - datetime.timedelta(1)
    cutoffDay = today - datetime.timedelta(days=daysToGoBack - 1)  # "- 1" to avoid fencepost error.

    ######enter your credentials######

    # certificate info that you get from search ads ui
    dprint('Certificates: pem="%s", key="%s".' % (pemPathname, keyPathname))

    ######make your api call######

    payload = {
        "startTime": str(cutoffDay),
        "endTime": str(today),
        "returnRowTotals": True,
        "returnRecordsWithNoMetrics": True,
        "selector": {
            "orderBy": [{"field": "localSpend", "sortOrder": "DESCENDING"}],
            "fields": ["localSpend", "taps", "impressions", "conversions", "avgCPA", "avgCPT", "ttr", "conversionRate"],
            "pagination": {"offset": 0, "limit": 1000}
        },
        # "groupBy"                    : [ "COUNTRY_CODE" ],
        # "granularity"                : 2, # 1 is hourly, 2 is daily, 3 is monthly etc
    }

    headers = {"Authorization": "orgId=%s" % orgId}

    dprint("Headers: %s\n" % headers)
    dprint("Payload: %s\n" % payload)
    # dprint("Apple URL: %s\n" % APPLE_KEYWORDS_REPORT_URL)

    response = getCampaignDataHelper(APPLE_KEYWORDS_REPORT_URL,
                                     cert=(pemPathname, keyPathname),
                                     json=payload,
                                     headers=headers)

    dprint("Response: '%s'" % response)
    return json.loads(response.text)


# ------------------------------------------------------------------------------
@debug
def createOneRowOfHistory(data):
    #  # Code below from https://stackoverflow.com/questions/2150739/iso-time-iso-8601-in-python, 13-Jan-2019
    #
    #  # UTC to ISO 8601 with TimeZone information
    #  utcTimestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    #
    #  # Local to ISO 8601 with TimeZone information
    #  # Calculate the offset taking into account daylight saving time
    #  utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    #  utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    #  timestamp = datetime.datetime.now().replace(tzinfo=datetime.timezone(offset=utc_offset)).isoformat()
    #
    #  return utcTimestamp, timestamp, str(data["spend"]), str(data["conversions"])
    print('createOneRowOfHistory:' + str(data))
    if int(data["conversions"]) > 0:
        return str(datetime.datetime.now().date()), \
           "%s" % round(data["spend"], 2), \
           str(data["conversions"]), \
           "%.2f" % round(data["spend"] / float(data["conversions"]), 2)
    else:
        print('createOneRowOfHistory:::adding line of history with 0s check values')
        return str(datetime.datetime.now().date()), \
           "%s" % round(0, 2), \
           str(data["conversions"]), \
           "%.2f" % round(0), 2


# ------------------------------------------------------------------------------
@debug
def createOneRowOfTable(data, label):
    cpi = "N/A" if data["conversions"] < 1 else ("{: 6,.2f}".format((0.0 + data["spend"]) / data["conversions"]))

    return """{:s}\t{:>9,.2f}\t{:>8,d}\t{:>s}""".format(label, data["spend"], data["conversions"], cpi)


# ------------------------------------------------------------------------------
@debug
def createEmailBodyForACampaign(client, summary, now):
    """Format:

  Timeframe        |    Cost    |  Installs  |  Cost per Install
  Yesterday        |  $     10  |        2   |   $  5
  Last Seven Days  |  $  1,000  |      100   |   $ 10
  All-Time         |  $ 10,000  |    5,000   |   $  2
"""

    return """\n""".join(["""Performance Summary\n""",
                          """\t""".join(["Timeframe\t", "   Cost", "\tInstalls", "Cost per Install"]),
                          createOneRowOfTable(summary[ONE_DAY], "Yesterday\t"),
                          createOneRowOfTable(summary[SEVEN_DAYS], "Last Seven Days"),
                          createOneRowOfTable(summary[FOUR_YEARS], "All-Time\t"),
                          """
Optimization Summary

Keyword bids updated today: %s
Adgroup bids updated today: %s
Keywords submitted for upload today: %s""" % \
                          # TODO rm v0 (client.updatedBids, client.updatedAdgroupBids, len(client.positiveKeywordsAdded))
                          (client.readUpdatedBidsCount(dynamodb), client.readUpdatedAdgroupBidsCount(dynamodb),
                           len(client.readPositiveKeywordsAdded(dynamodb)))
                          ])


# ------------------------------------------------------------------------------
@debug
def sendEmailForACampaign(client, emailBody, now):
    messageString = emailBody
    dateString = time.strftime("%m/%d/%Y", time.localtime(now))
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = EMAIL_SUBJECT % (client.clientName, dateString)

    #fullEmailList = EMAIL_TO + client.emailAddresses

    #print('sendEmailForACampaign:::fullEmailList' + str(fullEmailList))
    # TODO add sendG logic
    AdoyaEmail.sendEmailForACampaign(messageString, subjectString, client.emailAddresses, EMAIL_TO, EMAIL_FROM)


# msg = email.message.EmailMessage()
# msg.set_content(emailBody)
#
# dateString = time.strftime("%m/%d/%Y", time.localtime(now))
# if dateString.startswith("0"):
#   dateString = dateString[1:]
#
# msg['Subject'] = EMAIL_SUBJECT % (client.clientName, dateString)
# msg['From'] = EMAIL_FROM
# msg['To'] = client.emailAddresses
# msg['Bcc'] = EMAIL_BCC
# msg.replace_header("Content-Type", "text/html")

# msg.add_attachment("".join(client.getHistory(dynamodb)), filename="adoya.csv", subtype="csv")

# dprint("SMTP host/port=%s/%s" % (SMTP_HOSTNAME, SMTP_PORT))

# if sendG:
# with smtplib.SMTP(host=SMTP_HOSTNAME, port=SMTP_PORT) as smtpServer:
#   smtpServer.set_debuglevel(2)
#   smtpServer.starttls()
#   smtpServer.login(SMTP_USERNAME, SMTP_PASSWORD)
#   smtpServer.send_message(msg)

# else:
# with open("deleteme.email.txt", "w") as h:
#   h.write(msg.as_string())
# print("Not actually sending any email.  Saved message body in deleteme.email.txt")


# ------------------------------------------------------------------------------
@debug
def sendEmailReport(client, dataForVariousTimes):
    summary = {ONE_DAY: {"conversions": 0, "spend": 0.0},
               SEVEN_DAYS: {"conversions": 0, "spend": 0.0},
               FOUR_YEARS: {"conversions": 0, "spend": 0.0},
               }

    for someTime, campaignsForThatTime in dataForVariousTimes.items():
        summary[someTime] = {"conversions": 0,
                             "spend": 0.0}

        for campaign in campaignsForThatTime:
            totals = campaign["total"]
            conversions, spend = totals["conversions"], float(totals["localSpend"]["amount"])
            dprint("For %d (%s), campaign %s over %d days has %d conversions, %f spend." % \
                   (client.orgId, client.clientName, campaign["metadata"]["campaignId"], someTime, conversions, spend))
            summary[someTime]["conversions"] += totals["conversions"]
            summary[someTime]["spend"] += float(totals["localSpend"]["amount"])

    now = time.time()

    # TODO updated to use dynamo, no header row needed
    # client.addRowToHistory(createOneRowOfHistory(summary[ONE_DAY]),
    #                        ["Date", "Spend", "Conversions", "Cost per Install"])

    client.addRowToHistory(createOneRowOfHistory(summary[ONE_DAY]), dynamodb)
    emailBody = createEmailBodyForACampaign(client, summary, now)
    sendEmailForACampaign(client, emailBody, now)


# ------------------------------------------------------------------------------
@debug
def process():
    for client in CLIENTS:
        dataForVariousTimes = {}

        for daysToGoBack in (ONE_DAY, SEVEN_DAYS, FOUR_YEARS):
            campaignData = getCampaignData(client.orgId,
                                           client.pemPathname,
                                           client.keyPathname,
                                           daysToGoBack)

            dataArray = campaignData["data"]["reportingDataResponse"]["row"]
            print('runClientDailyReport:::dataArray' + str(dataArray))

            dprint("For %d (%s), there are %d campaigns in the campaign data." % \
                   (client.orgId, client.clientName, len(dataArray)))

            dataForVariousTimes[daysToGoBack] = dataArray

        print('runClientDailyReport:::dataForVariousTimes' + str(dataForVariousTimes))
        sendEmailReport(client, dataForVariousTimes)


# ------------------------------------------------------------------------------
@debug
def terminate():
    pass


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
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
        'body': json.dumps('Run Client Daily Report Complete')
    }