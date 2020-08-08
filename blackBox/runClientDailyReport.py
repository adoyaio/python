import datetime
import json
import logging
import time
import boto3
import requests
from configuration import config
from debug import debug, dprint
from retry import retry
from utils import EmailUtils, DynamoUtils, S3Utils
from Client import Client

ONE_DAY = 1
SEVEN_DAYS = 7
THIRTY_DAYS = 30  # JF with branch integration using this rather than 4
FOUR_YEARS = 365 * 4  # Ignoring leap years.
EMAIL_SUBJECT = """%s - Apple Search Ads Update %s"""

logger = logging.getLogger()
sendG = False  # Set to True to enable sending email to clients, else a test run.

@debug
def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global clientsG
    global dynamodb
    global EMAIL_TO
    EMAIL_TO = emailToInternal

    if env == "lcl":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
        logger.setLevel(logging.INFO)
    elif env == "prod":
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)  # reduce AWS logging in production
        # debug.disableDebug()  disable debug wrappers in production
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)

    clientsG = Client.getClients(dynamodb)
    logger.info("runClientDailyReport:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


@retry
def getCampaignDataHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)


def getCampaignData(orgId, pemFilename, keyFilename, daysToGoBack):
    today = datetime.date.today() - datetime.timedelta(1)
    cutoffDay = today - datetime.timedelta(days=daysToGoBack - 1)  # TODO revisit "- 1" to avoid fencepost error.
    payload = {
        "startTime": str(cutoffDay),
        "endTime": str(today),
        "returnRowTotals": True,
        "returnRecordsWithNoMetrics": True,
        "selector": {
            "orderBy": [{"field": "localSpend", "sortOrder": "DESCENDING"}],
            "fields": ["localSpend", "taps", "impressions", "installs", "avgCPA", "avgCPT", "ttr", "conversionRate"],
            "pagination": {"offset": 0, "limit": 1000}
        },
        # "groupBy"                    : [ "COUNTRY_CODE" ],
        # "granularity"                : 2, # 1 is hourly, 2 is daily, 3 is monthly etc
    }

    headers = {"Authorization": "orgId=%s" % orgId}

    dprint("\n\nHeaders: %s" % headers)
    dprint("\n\nPayload: %s" % payload)
    dprint("\n\nApple URL: %s" % config.APPLE_KEYWORDS_REPORT_URL)

    response = getCampaignDataHelper(config.APPLE_KEYWORDS_REPORT_URL,
                                     cert=(S3Utils.getCert(pemFilename),
                                           S3Utils.getCert(keyFilename)),
                                     json=payload,
                                     headers=headers)

    dprint("\n\nResponse: '%s'" % response)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return False


@debug
def createOneRowOfHistory(data): # eg {'installs': 88, 'spend': 115.92, 'purchases': 9, 'revenue': 204.0}  
    
    # asa fields
    timestamp = str(datetime.datetime.now().date())
    installs = data["installs"]
    spend = data["spend"] 
    if int(installs) > 0:
        spend = "%s" % round(spend, 2)
        cpi = "%.2f" % round(float(spend) / float(installs), 2)
    else:
        spend = "%.2f" % 0
        cpi = "%.2f" % 0

    # branch fields
    purchases =  data["purchases"]
    revenue = data["revenue"]
    if int(purchases) > 0 and float(spend) > 0:   
        cpp = "%.2f" % round(float(spend) / float(purchases), 2)
    else:
        cpp = "%.2f" % 0

    if float(revenue) > 0 and float(spend) > 0:
        revenueOverCost = "%.2f" % round(float(revenue) / float(spend), 2)
        revenue = "%.2f" % round(revenue, 2)
    else:    
        revenueOverCost = "%.2f" % 0
        revenue = "%.2f" % 0

    return timestamp, spend, installs, cpi, purchases, revenue, cpp, revenueOverCost


# JF TODO non html email should be updated with branch data
def createOneRowOfTable(data, label):
    cpi = "N/A" if data["installs"] < 1 else ("{: 6,.2f}".format((0.0 + data["spend"]) / data["installs"]))

    return """{:s}\t{:>9,.2f}\t{:>8,d}\t{:>s}""".format(label, data["spend"], data["installs"], cpi)


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
                          createOneRowOfTable(summary[THIRTY_DAYS], "Last Thirty Days")
                          ])

def createHtmlEmailBodyForACampaign(client, summary, now):
    if client.currency == 'USD':
        currencySymbol = '$'
    elif client.currency == 'EUR':
        currencySymbol = '€'

    # gather values for the html report and replace placeholder values
    cpiOneDay = "N/A" if summary[ONE_DAY]["installs"] < 1 else ("{: 6,.2f}".format((0.0 + summary[ONE_DAY]["spend"]) / summary[ONE_DAY]["installs"]))
    installsOneDay = "{: 6,.0f}".format(summary[ONE_DAY]["installs"])
    spendOneDay = "{:>9,.2f}".format(summary[ONE_DAY]["spend"])

    cpiSevenDays = "N/A" if summary[SEVEN_DAYS]["installs"] < 1 else ("{: 6,.2f}".format((0.0 + summary[SEVEN_DAYS]["spend"]) / summary[SEVEN_DAYS]["installs"]))
    installsSevenDays = "{: 6,.0f}".format(summary[SEVEN_DAYS]["installs"])
    spendSevenDays = "{:>9,.2f}".format(summary[SEVEN_DAYS]["spend"])

    cpiFourYears = "N/A" if summary[THIRTY_DAYS]["installs"] < 1 else ("{: 6,.2f}".format((0.0 + summary[THIRTY_DAYS]["spend"]) / summary[THIRTY_DAYS]["installs"]))
    installsFourYears = "{: 6,.0f}".format(summary[THIRTY_DAYS]["installs"])

    spendFourYears = "{:>9,.2f}".format(summary[THIRTY_DAYS]["spend"])

    # gather branch metrics for post install summary
    cppOneDay = "N/A" if summary[ONE_DAY]["purchases"] < 1 else ("{: 6,.2f}".format((0.0 + summary[ONE_DAY]["spend"]) / summary[ONE_DAY]["purchases"]))
    revenueCostOneDay = "N/A" if summary[ONE_DAY]["revenue"] < 1 else (
        "{: 6,.2f}".format((0.0 + summary[ONE_DAY]["revenue"]) / summary[ONE_DAY]["spend"]))
    purchaseOneDay = "{: 6,.0f}".format(summary[ONE_DAY]["purchases"])
    revenueOneDay = "{:>9,.2f}".format(summary[ONE_DAY]["revenue"])

    cppSevenDays = "N/A" if summary[SEVEN_DAYS]["purchases"] < 1 else ("{: 6,.2f}".format((0.0 + summary[SEVEN_DAYS]["spend"]) / summary[SEVEN_DAYS]["purchases"]))
    revenueCostSevenDays = "N/A" if summary[SEVEN_DAYS]["revenue"] < 1 else (
        "{: 6,.2f}".format((0.0 + summary[SEVEN_DAYS]["revenue"]) / summary[SEVEN_DAYS]["spend"]))
    purchaseSevenDays = "{: 6,.0f}".format(summary[SEVEN_DAYS]["purchases"])
    revenueSevenDays = "{:>9,.2f}".format(summary[SEVEN_DAYS]["revenue"])

    cppFourYears = "N/A" if summary[THIRTY_DAYS]["purchases"] < 1 else ("{: 6,.2f}".format((0.0 + summary[THIRTY_DAYS]["spend"]) / summary[THIRTY_DAYS]["purchases"]))
    revenueCostFourYears = "N/A" if summary[THIRTY_DAYS]["revenue"] < 1 else (
        "{: 6,.2f}".format((0.0 + summary[THIRTY_DAYS]["revenue"]) / summary[THIRTY_DAYS]["spend"]))
    purchaseFourYears = "{: 6,.0f}".format(summary[THIRTY_DAYS]["purchases"])
    revenueFourYears = "{:>9,.2f}".format(summary[THIRTY_DAYS]["revenue"])
    htmlBody = ""

    # read email template and replace values
    f = open("./assets/email_template.html", "r")
    for x in f:
        
        # install summary
        x = x.replace("@@YESTERDAY__COST@@", str(currencySymbol+spendOneDay))
        x = x.replace("@@YESTERDAY__INSTALLS@@", str(installsOneDay))
        x = x.replace("@@YESTERDAY__CPI@@", str(currencySymbol+cpiOneDay))

        x = x.replace("@@SEVEN__DAYS__COST@@", str(currencySymbol+spendSevenDays))
        x = x.replace("@@SEVEN__DAYS__INSTALLS@@", str(installsSevenDays))
        x = x.replace("@@SEVEN__DAYS__CPI@@", str(currencySymbol+cpiSevenDays))

        x = x.replace("@@ALL__TIME__COST@@", str(currencySymbol+spendFourYears))
        x = x.replace("@@ALL__TIME__INSTALLS@@", str(installsFourYears))
        x = x.replace("@@ALL__TIME__CPI@@", str(currencySymbol+cpiFourYears))

        # post-install summary
        x = x.replace("@@YESTERDAY__PURCHASE@@", str(purchaseOneDay))
        x = x.replace("@@YESTERDAY__REVENUE@@", str(currencySymbol+revenueOneDay))

        if(cppOneDay == "N/A"):
            x = x.replace("@@YESTERDAY__CPP@@", str(cppOneDay))
        else:
            x = x.replace("@@YESTERDAY__CPP@@", str(currencySymbol + cppOneDay))

        if(revenueCostOneDay == 'N/A'):
            x = x.replace("@@YESTERDAY__REVENUE__COST@@", str(revenueCostOneDay))
        else:
            x = x.replace("@@YESTERDAY__REVENUE__COST@@", str(currencySymbol + revenueCostOneDay))

        x = x.replace("@@SEVEN__DAYS__PURCHASE@@", str(purchaseSevenDays))
        x = x.replace("@@SEVEN__DAYS__REVENUE@@", str(currencySymbol+revenueSevenDays))

        if(cppSevenDays == "N/A"):
            x = x.replace("@@SEVEN__DAYS__CPP@@", str(cppSevenDays))
        else:
            x = x.replace("@@SEVEN__DAYS__CPP@@", str(currencySymbol + cppSevenDays))

        if(revenueCostSevenDays == "N/A"):
            x = x.replace("@@SEVEN__DAYS__REVENUE__COST@@", str(revenueCostSevenDays))
        else:
            x = x.replace("@@SEVEN__DAYS__REVENUE__COST@@", str(currencySymbol+revenueCostSevenDays))

        x = x.replace("@@ALL__TIME__PURCHASE@@", str(purchaseFourYears))
        x = x.replace("@@ALL__TIME__REVENUE@@", str(currencySymbol+revenueFourYears))

        if(cppFourYears == "N/A"):
            x = x.replace("@@ALL__TIME__CPP@@", str(cppFourYears))
        else:
            x = x.replace("@@ALL__TIME__CPP@@", str(currencySymbol + cppFourYears))

        if (revenueCostFourYears == "N/A"):
            x = x.replace("@@ALL__TIME__REVENUE__COST@@", str(revenueCostFourYears))
        else:
            x = x.replace("@@ALL__TIME__REVENUE__COST@@", str(currencySymbol+revenueCostFourYears))

        # JF NOTE release 2 unused optimization summary
        # x = x.replace("@@KEYWORD__BIDS__TODAY@@", str(client.readUpdatedBidsCount(dynamodb)))
        # x = x.replace("@@ADGROUP__BIDS__TODAY@@", str(client.readUpdatedAdgroupBidsCount(dynamodb)))
        # x = x.replace("@@KEYWORDS__TODAY@@", str(len(client.readPositiveKeywordsAdded(dynamodb))))
        htmlBody = htmlBody + x

    return htmlBody


def sendEmailForACampaign(client, emailBody, htmlBody, now):
    dateString = time.strftime("%m/%d/%Y", time.localtime(now))
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = EMAIL_SUBJECT % (client.clientName, dateString)

    if sendG:
        EmailUtils.sendEmailForACampaign(emailBody, 
        htmlBody, 
        subjectString, 
        client.emailAddresses, 
        EMAIL_TO,
        config.EMAIL_FROM)
    else:
        EmailUtils.sendEmailForACampaign(emailBody, 
        htmlBody, 
        subjectString, 
        ['test@adoya.io'], 
        EMAIL_TO, 
        config.EMAIL_FROM)


def sendEmailReport(client, dataForVariousTimes):
    today = datetime.date.today()
    summary = {ONE_DAY: {"installs": 0, "spend": 0.0, "purchases": 0, "revenue": 0.0},
               SEVEN_DAYS: {"installs": 0, "spend": 0.0, "purchases": 0, "revenue": 0.0},
               THIRTY_DAYS: {"installs": 0, "spend": 0.0, "purchases": 0, "revenue": 0.0}
               }
    for someTime, campaignsForThatTime in dataForVariousTimes.items():
        summary[someTime] = {"installs": 0, "spend": 0.0}

        # Iterate each campaign and get totals
        for campaign in campaignsForThatTime:
            totals = campaign["total"]
            installs, spend = totals["installs"], float(totals["localSpend"]["amount"])
            dprint("For %d (%s), campaign %s over %d days has %d installs, %f spend." % \
                   (client.orgId, client.clientName, campaign["metadata"]["campaignId"], someTime, installs, spend))
            summary[someTime]["installs"] += totals["installs"]
            summary[someTime]["spend"] += float(totals["localSpend"]["amount"])

        # Add branch events 
        # TODO get total BranchEvents, not limited to campaign
        end_date_delta = datetime.timedelta(days=1)
        start_date_delta = datetime.timedelta(days=someTime)
        start_date = today - start_date_delta
        end_date = today - end_date_delta
        print("branch end_date:::" + str(end_date))
        print("branch start_date:::" + str(start_date))
        summary[someTime]["purchases"] = client.getTotalBranchEvents(dynamodb, start_date, end_date)
        summary[someTime]["revenue"] = client.getTotalBranchRevenue(dynamodb, start_date, end_date)

    now = time.time()
    client.addRowToHistory(createOneRowOfHistory(summary[ONE_DAY]), dynamodb, end_date)
    emailBody = createEmailBodyForACampaign(client, summary, now)
    htmlBody = createHtmlEmailBodyForACampaign(client, summary, now)
    sendEmailForACampaign(client, emailBody, htmlBody, now)


@debug
def process():
    for client in clientsG:
        dataForVariousTimes = {}

        for daysToGoBack in (ONE_DAY, SEVEN_DAYS, THIRTY_DAYS):
            campaignData = getCampaignData(client.orgId,
                                           client.pemFilename,
                                           client.keyFilename,
                                           daysToGoBack)

            if(campaignData != False):
                dataArray = campaignData["data"]["reportingDataResponse"]["row"]
                dprint("For %d (%s), there are %d campaigns in the campaign data." % \
                    (client.orgId, client.clientName, len(dataArray)))
                dataForVariousTimes[daysToGoBack] = dataArray

        sendEmailReport(client, dataForVariousTimes)

@debug
def terminate():
    pass


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