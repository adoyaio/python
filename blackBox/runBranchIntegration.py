from __future__ import print_function # Python 2/3 compatibility
import boto3
from collections import defaultdict
import datetime
import email.message
from email.headerregistry import Address
import json
import os
import pandas as pd
import pprint
import requests
import smtplib
import sys
import time

from botocore.exceptions import ClientError

from Client import CLIENTS
from configuration import SMTP_HOSTNAME, \
    SMTP_PORT, \
    SMTP_USERNAME, \
    SMTP_PASSWORD, \
    EMAIL_FROM, \
    APPLE_UPDATE_POSITIVE_KEYWORDS_URL, \
    APPLE_KEYWORD_REPORTING_URL_TEMPLATE, \
    TOTAL_COST_PER_INSTALL_LOOKBACK, \
    HTTP_REQUEST_TIMEOUT, \
    BRANCH_ANALYTICS_URL_BASE

from debug import debug, dprint
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

###### date and time parameters for bidding lookback ######
BIDDING_LOOKBACK = 7  # days
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
    logger.info("In initialize(), getcwd()='%s' and sendG=%s." % (os.getcwd(), sendG))


# ------------------------------------------------------------------------------
def getKeywordReportFromBranchHelper(url, json, headers):
    return requests.post(url, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)


# ------------------------------------------------------------------------------
@debug
def getKeywordReportFromBranch():
    payload = {
        "branch_key": "key_live_bdnGY11GPAUIkCEibh6z1adlCzbm09Vf",
        "branch_secret": "secret_live_6afuIbahkAfjz4mChpHvf6TInRM33EGb",
        "start_date": str(start_date),
        "end_date": str(end_date),
        "data_source": "eo_commerce_event",
        "dimensions": [
            "last_attributed_touch_data_tilde_feature",
            "last_attributed_touch_data_tilde_channel",
            "last_attributed_touch_data_tilde_campaign",
            "last_attributed_touch_data_tilde_keyword",
            "last_attributed_touch_data_tilde_ad_name",
            "last_attributed_touch_data_tilde_ad_id",
            "last_attributed_touch_data_tilde_campaign_id",
            "last_attributed_touch_data_tilde_advertising_partner_name",
            "last_attributed_touch_data_tilde_ad_set_id",
            "last_attributed_touch_data_tilde_ad_set_name"
        ],
        "granularity": "day", "aggregation": "total_count",
        "filters": {
            "last_attributed_touch_data_tilde_feature":
                [
                    "paid advertising"
                ],
            "last_attributed_touch_data_tilde_advertising_partner_name":
                [
                    "Apple Search Ads"
                ]
        },
        "zero_fill": "true"
    }

    url: str = BRANCH_ANALYTICS_URL_BASE

    headers = {"Content-Type": "application/json"}

    logger.info("URL is '%s'." % url)
    logger.info("Payload is '%s'." % payload)
    logger.info("Headers are %s." % headers)

    response = getKeywordReportFromBranchHelper(url, json=payload, headers=headers)
    logger.info("Response is %s." % response)

    return json.loads(response.text)

# ------------------------------------------------------------------------------
@debug
def process():

#summaryReportInfo = {}

# for client in CLIENTS:
# summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = clientSummaryReportInfo = { }
# campaignIds = client.campaignIds

# for campaignId in campaignIds:

    data = getKeywordReportFromBranch()
    #
    #dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('branch_commerce_events')
    if table:
        logger.info("found table")
    else:
        logger.info("issue connecting to dynamodb ")

    results = data['results']
    if len(results) == 0:
        return False  # EARLY RETURN

    for result in results:

        if 'last_attributed_touch_data_tilde_campaign' in result["result"]:

            dash = "-"
            timestamp = str(result["timestamp"])
            campaign = str(result["result"]["last_attributed_touch_data_tilde_campaign"])
            campaign_id = str(result["result"]["last_attributed_touch_data_tilde_campaign_id"])
            keyword = str(result["result"]["last_attributed_touch_data_tilde_keyword"])
            ad_set_id = str(result["result"]["last_attributed_touch_data_tilde_ad_set_id"])
            ad_set_name = str(result["result"]["last_attributed_touch_data_tilde_ad_set_name"])
            count = str(result["result"]["total_count"])

            if(campaign == "exact_match"):
                branch_commerce_event_key = campaign_id + dash + ad_set_id + dash + keyword.replace(" ", dash)
            else:
                branch_commerce_event_key = campaign_id + dash + ad_set_name

                # enable for local debugging
                # dprint("timestamp=%s." % timestamp)
                # dprint("campaign=%s." % campaign)
                # dprint("keyword=%s." % keyword)
                # dprint("count=%s." % count)
                # dprint("campaign_id=%s." % campaign_id)
                # dprint("branch_commerce_event_key=%s." % branch_commerce_event_key)

            try:
                response = table.put_item(
                    Item={
                        'branch_commerce_event_key': branch_commerce_event_key,
                        'timestamp': timestamp,
                        'campaign': campaign,
                        'campaign_id': campaign_id,
                        'keyword': keyword,
                        'ad_set_id':  ad_set_id,
                        'ad_set_name': ad_set_name,
                        'count': count
                    }
                )
            except ClientError as e:
                logger.info("PutItem failed due to" + e.response['Error']['Message'])
            else:
                logger.info("PutItem succeeded:")
        else:
            logger.info("Non keyword branch item found, skipping")
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

def lambda_handler(event, context):
    initialize()
    process()
    terminate()
    return {
        'statusCode': 200,
        'body': json.dumps('Run Branch Integration Complete')
    }