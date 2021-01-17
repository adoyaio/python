import datetime
import json
import logging
import time
import boto3
import requests
import sys
from configuration import config
from utils.debug import debug, dprint
from utils.retry import retry
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
from Client import Client

ONE_DAY = 1
SEVEN_DAYS = 7
THIRTY_DAYS = 30  # JF with branch integration using this rather than 4
FOUR_YEARS = 365 * 4  # Ignoring leap years.
EMAIL_SUBJECT = """%s - Apple Search Ads Update %s"""


def initialize(clientEvent):
    global emailClientsG
    emailClientsG = LambdaUtils.getEmailClientsG(
        clientEvent['rootEvent']['env']
    )

    global sendG
    global clientG
    global emailToG
    global dynamodb
    global logger

    emailToG = clientEvent['rootEvent']['emailToInternal']
    sendG = LambdaUtils.getSendG(
        clientEvent['rootEvent']['env']
    )
    dynamodb = LambdaUtils.getDynamoResource(
        clientEvent['rootEvent']['env'],
        clientEvent['rootEvent']['dynamoEndpoint']
    )
    orgDetails = json.loads(clientEvent['orgDetails'])
    clientG = Client(
        orgDetails['_orgId'],
        orgDetails['_clientName'],
        orgDetails['_emailAddresses'],
        orgDetails['_keyFilename'],
        orgDetails['_pemFilename'],
        orgDetails['_bidParameters'],
        orgDetails['_adgroupBidParameters'],
        orgDetails['_branchBidParameters'],
        orgDetails['_appleCampaigns'],
        orgDetails['_keywordAdderParameters'],
        orgDetails['_branchIntegrationParameters'],
        orgDetails['_currency'],
        orgDetails['_appName'],
        orgDetails['_appID'],
        orgDetails['_campaignName']
    )
    logger = LambdaUtils.getLogger(clientEvent['rootEvent']['env'])  
    logger.info("runClientDailyReport:::initialize(), rootEvent='" + str(clientEvent['rootEvent']))

@retry
def getCampaignDataHelper(url, cert, json, headers):
    return requests.post(url, cert=cert, json=json, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)


def getCampaignData(daysToGoBack):
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

    url: str = config.APPLE_KEYWORDS_REPORT_URL
    headers = {"Authorization": "orgId=%s" % clientG.orgId}
    dprint("\n\nHeaders: %s" % headers)
    dprint("\n\nPayload: %s" % payload)
    dprint("\n\nApple URL: %s" % url)

    response = getCampaignDataHelper(
        url,
        cert=(S3Utils.getCert(clientG.pemFilename), S3Utils.getCert(clientG.keyFilename)),
        json=payload,
        headers=headers
    )
    dprint("\n\nResponse: '%s'" % response)

    # TODO extract to utils
    if response.status_code != 200:
        email = "client id:%d \n url:%s \n payload:%s \n response:%s" % (clientG.orgId, url, payload, response)
        date = time.strftime("%m/%d/%Y")
        subject ="%s - %d ERROR in runClientDailyReport for %s" % (date, response.status_code, clientG.clientName)
        logger.warn(email)
        logger.error(subject)
        if sendG:
            EmailUtils.sendTextEmail(email, subject, emailToG, [], config.EMAIL_FROM)
        
        return False

    return json.loads(response.text)

# JF TODO non html email should be updated with branch data
def createOneRowOfTable(data, label):
    cpi = "N/A" if data["installs"] < 1 else ("{: 6,.2f}".format((0.0 + data["spend"]) / data["installs"]))

    return """{:s}\t{:>9,.2f}\t{:>8,d}\t{:>s}""".format(label, data["spend"], data["installs"], cpi)


def createEmailBodyForACampaign(summary, now):
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

def createHtmlEmailBodyForACampaign(summary, now):
    if clientG.currency == 'USD':
        currencySymbol = '$'
    elif clientG.currency == 'EUR':
        currencySymbol = '€'

    # normalize fields for float based formatting
    summary[ONE_DAY]["cpi"] = float(summary[ONE_DAY]["cpi"])
    summary[ONE_DAY]["cpp"] = float(summary[ONE_DAY]["cpp"])
    summary[ONE_DAY]["revenueOverCost"] = float(summary[ONE_DAY]["revenueOverCost"])
    summary[ONE_DAY]["revenue"] = float(summary[ONE_DAY]["revenue"])

    summary[SEVEN_DAYS]["cpi"] = float(summary[SEVEN_DAYS]["cpi"])
    summary[SEVEN_DAYS]["cpp"] = float(summary[SEVEN_DAYS]["cpp"])
    summary[SEVEN_DAYS]["revenueOverCost"] = float(summary[SEVEN_DAYS]["revenueOverCost"])
    summary[SEVEN_DAYS]["revenue"] = float(summary[SEVEN_DAYS]["revenue"])

    summary[THIRTY_DAYS]["cpi"] = float(summary[THIRTY_DAYS]["cpi"])
    summary[THIRTY_DAYS]["cpp"] = float(summary[THIRTY_DAYS]["cpp"])  
    summary[THIRTY_DAYS]["revenueOverCost"] = float(summary[THIRTY_DAYS]["revenueOverCost"]) 
    summary[THIRTY_DAYS]["revenue"] = float(summary[THIRTY_DAYS]["revenue"])

    # format apple fields for the html report
    cpiOneDay = "{: 6,.2f}".format(summary[ONE_DAY]["cpi"] )if summary[ONE_DAY]["cpi"] > .01 else "N/A"
    installsOneDay = "{: 6,.0f}".format(summary[ONE_DAY]["installs"])
    spendOneDay = "{:>9,.2f}".format(summary[ONE_DAY]["spend"])
    
    cpiSevenDays =  "{: 6,.2f}".format(summary[SEVEN_DAYS]["cpi"]) if summary[SEVEN_DAYS]["installs"] > .01 else "N/A"
    installsSevenDays = "{: 6,.0f}".format(summary[SEVEN_DAYS]["installs"])
    spendSevenDays = "{:>9,.2f}".format(summary[SEVEN_DAYS]["spend"])
    
    cpiFourYears ="{: 6,.2f}".format(summary[THIRTY_DAYS]["cpi"]) if summary[THIRTY_DAYS]["installs"] > .01 else "N/A"
    installsFourYears = "{: 6,.0f}".format(summary[THIRTY_DAYS]["installs"])
    spendFourYears = "{:>9,.2f}".format(summary[THIRTY_DAYS]["spend"])

    # format branch metrics for post install summary
    cppOneDay = "{: 6,.2f}".format(summary[ONE_DAY]["cpp"]) if summary[ONE_DAY]["cpp"] > .01 else "N/A"
    revenueCostOneDay = "{: 6,.2f}".format(summary[ONE_DAY]["revenueOverCost"]) if summary[ONE_DAY]["revenueOverCost"] > .01 else "N/A"
    purchaseOneDay =  "{: 6,.0f}".format(summary[ONE_DAY]["purchases"])
    revenueOneDay = "{: 9,.2f}".format(summary[ONE_DAY]["revenue"])
    
    cppSevenDays = "{: 6,.2f}".format(summary[SEVEN_DAYS]["cpp"]) if summary[SEVEN_DAYS]["cpp"] > .01 else "N/A"
    revenueCostSevenDays = "{: 6,.2f}".format(summary[SEVEN_DAYS]["revenueOverCost"]) if summary[SEVEN_DAYS]["revenueOverCost"] > .01 else "N/A"
    purchaseSevenDays = "{: 6,.0f}".format(summary[SEVEN_DAYS]["purchases"])
    revenueSevenDays = "{: 6,.2f}".format(summary[SEVEN_DAYS]["revenue"])
    
    cppFourYears =  "{: 6,.2f}".format(summary[THIRTY_DAYS]["cpp"]) if summary[THIRTY_DAYS]["cpp"] > .01 else "N/A"
    revenueCostFourYears = "{: 6,.2f}".format(summary[THIRTY_DAYS]["revenueOverCost"]) if summary[THIRTY_DAYS]["revenueOverCost"] > .01 else "N/A"
    purchaseFourYears = "{: 6,.0f}".format(summary[THIRTY_DAYS]["purchases"])
    revenueFourYears = "{: 6,.2f}".format(summary[THIRTY_DAYS]["revenue"])

    # normalize for string replacing
    cpiOneDay = str(cpiOneDay)
    cppOneDay = str(cppOneDay)
    revenueCostOneDay = str(revenueCostOneDay)
    revenueOneDay = str(revenueOneDay)

    cpiSevenDays = str(cpiSevenDays)
    cppSevenDays = str(cppSevenDays)
    revenueCostSevenDays = str(revenueCostSevenDays)
    revenueSevenDays = str(revenueSevenDays)
    
    cpiFourYears = str(cpiFourYears)
    cppFourYears = str(cppFourYears)
    revenueCostFourYears = str(revenueCostFourYears)  
    revenueFourYears = str(revenueFourYears)

    # read email template / replace values
    htmlBody = ""
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


def sendEmailForACampaign(emailBody, htmlBody, now):
    dateString = time.strftime("%m/%d/%Y", time.localtime(now))
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = EMAIL_SUBJECT % (clientG.clientName, dateString)

    if emailClientsG:
        EmailUtils.sendEmailForACampaign(
            emailBody, 
            htmlBody, 
            subjectString, 
            clientG.emailAddresses, 
            emailToG,
            config.EMAIL_FROM
        )
    else:
        EmailUtils.sendEmailForACampaign(
            emailBody, 
            htmlBody, 
            subjectString, 
            ['test@adoya.io'], 
            emailToG, 
            config.EMAIL_FROM
        )


def sendEmailReport(dataForVariousTimes):
    today = datetime.date.today()
    now = time.time()
    timestamp = str(datetime.datetime.now().date())

    summary = {
        ONE_DAY: {
            "installs": 0, 
            "spend": 0.0, 
            "purchases": 0, 
            "revenue": 0.0
            },
        SEVEN_DAYS: {
            "installs": 0, 
            "spend": 0.0, 
            "purchases": 0, 
            "revenue": 0.0
        },
        THIRTY_DAYS: {
            "installs": 0, 
            "spend": 0.0, 
            "purchases": 0, 
            "revenue": 0.0
        }
    }
    
    for someTime, campaignsForThatTime in dataForVariousTimes.items():
        summary[someTime] = {"installs": 0, "spend": 0.0}
        
        # iterate each campaign and get totals
        for campaign in campaignsForThatTime:
            
            # pull install and spend from asa response
            campaignId = campaign["metadata"]["campaignId"]
            installs, spend = campaign["total"]["installs"], float(campaign["total"]["localSpend"]["amount"])
            
            # increment install and spend for totals
            summary[someTime]["installs"] += installs
            summary[someTime]["spend"] += spend

            # calculate campaign level cpi etc 
            if str(campaignId) == clientG.keywordAdderIds.get("campaignId").get("search"):
                summary[someTime]["cpi_search"] = clientG.calculateCPI(spend, installs)
                summary[someTime]["installs_search"] = installs
                summary[someTime]["spend_search"] = spend

            if str(campaignId) == clientG.keywordAdderIds.get("campaignId").get("broad"):
                summary[someTime]["cpi_broad"] = clientG.calculateCPI(spend, installs)
                summary[someTime]["installs_broad"] = installs
                summary[someTime]["spend_broad"] = spend
            
            if str(campaignId) == clientG.keywordAdderIds.get("campaignId").get("exact"):
                summary[someTime]["cpi_exact"] = clientG.calculateCPI(spend, installs)
                summary[someTime]["installs_exact"] = installs
                summary[someTime]["spend_exact"] = spend

            if str(campaignId) == clientG.keywordAdderIds.get("campaignId").get("brand"):
                summary[someTime]["cpi_brand"] = clientG.calculateCPI(spend, installs)
                summary[someTime]["installs_brand"] = installs
                summary[someTime]["spend_brand"] = spend
       
        
        # calculate total cpi for timeperiod and put it on the summary object
        summary[someTime]["cpi"] = clientG.calculateCPI(
            summary[someTime]["spend"], 
            summary[someTime]["installs"]
        )

        # calculate branch metrics for this timeperiod 
        end_date_delta = datetime.timedelta(days=1)
        end_date = today - end_date_delta 
        start_date_delta = datetime.timedelta(days=someTime)
        start_date = today - start_date_delta
       
        purchases = clientG.getTotalBranchEvents(
            dynamodb, 
            start_date, 
            end_date
        )
        revenue = clientG.getTotalBranchRevenue(
            dynamodb, 
            start_date, 
            end_date
        )

        # for this timeslice format revenue, calculate cpp and r/c
        revenue, cpp, revenueOverCost = clientG.calculateBranchMetrics(
            summary[someTime]["spend"],
            purchases,
            revenue
        )

        # put it on the summary object
        summary[someTime]["revenue"] = revenue
        summary[someTime]["cpp"] = cpp
        summary[someTime]["revenueOverCost"] = revenueOverCost
        summary[someTime]["purchases"] = purchases

    emailBody = createEmailBodyForACampaign(summary, now)
    htmlBody = createHtmlEmailBodyForACampaign(summary, now)
    sendEmailForACampaign(emailBody, htmlBody, now)

    # cast to string where needed to avoid dynamo float/decimal issues
    rowOfHistory = {}
    rowOfHistory["spend"] = str(round(summary[ONE_DAY].get("spend"),2))
    rowOfHistory["spend_exact"] = str(round(summary[ONE_DAY].get("spend_exact",0),2))
    rowOfHistory["spend_search"] = str(round(summary[ONE_DAY].get("spend_search",0),2))
    rowOfHistory["spend_broad"] = str(round(summary[ONE_DAY].get("spend_broad",0),2))
    rowOfHistory["spend_brand"] = str(round(summary[ONE_DAY].get("spend_brand",0),2))

    rowOfHistory["installs"] = summary[ONE_DAY].get("installs")
    rowOfHistory["installs_exact"] = summary[ONE_DAY].get("installs_exact",0)
    rowOfHistory["installs_search"] = summary[ONE_DAY].get("installs_search",0)
    rowOfHistory["installs_broad"] = summary[ONE_DAY].get("installs_broad",0)
    rowOfHistory["installs_brand"] = summary[ONE_DAY].get("installs_brand",0)

    rowOfHistory["cpi"] = str(summary[ONE_DAY].get("cpi"))
    rowOfHistory["cpi_exact"] = str(summary[ONE_DAY].get("cpi_exact", 0.00))
    rowOfHistory["cpi_broad"] = str(summary[ONE_DAY].get("cpi_broad", 0.00))
    rowOfHistory["cpi_search"] = str(summary[ONE_DAY].get("cpi_search", 0.00))
    rowOfHistory["cpi_brand"] = str(summary[ONE_DAY].get("cpi_brand", 0.00))

    rowOfHistory["purchases"] = summary[ONE_DAY].get("purchases")
    rowOfHistory["revenue"] = str(summary[ONE_DAY].get("revenue"))
    rowOfHistory["cpp"] = str(summary[ONE_DAY].get("cpp"))
    rowOfHistory["revenueOverCost"] = str(summary[ONE_DAY].get("revenueOverCost"))

    clientG.addRowToHistory(rowOfHistory, dynamodb, end_date)

def process():
    print("runClientDailyReport:::" + clientG.clientName + ":::" + str(clientG.orgId))
    dataForVariousTimes = {}
    for daysToGoBack in (ONE_DAY, SEVEN_DAYS, THIRTY_DAYS):
        campaignData = getCampaignData(
            daysToGoBack
        )
        if(campaignData != False):
            dataArray = campaignData["data"]["reportingDataResponse"]["row"]
            dprint("For %d (%s), there are %d campaigns in the campaign data." % \
                (clientG.orgId, clientG.clientName, len(dataArray)))
            dataForVariousTimes[daysToGoBack] = dataArray
    dataForOneDay = dataForVariousTimes.get(ONE_DAY, None)
    dataForSevenDay = dataForVariousTimes.get(SEVEN_DAYS, None)
    dataForThirtyDay = dataForVariousTimes.get(THIRTY_DAYS, None)
    
    if not dataForOneDay or not dataForSevenDay or not dataForThirtyDay:
        return
    sendEmailReport(dataForVariousTimes)


if __name__ == "__main__":
    clientEvent = LambdaUtils.getClientForLocalRun(
        int(sys.argv[1]),
        ['james@adoya.io']
    )
    initialize(clientEvent)
    process()


def lambda_handler(clientEvent):
    initialize(clientEvent)
    process()

    return {
        'statusCode': 200,
        'body': json.dumps('Run Client Daily Report Complete for ' + clientG.clientName)
    }