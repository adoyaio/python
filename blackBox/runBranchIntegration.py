from __future__ import print_function  # Python 2/3 compatibility

import datetime
import decimal
import json
import logging

import boto3
import requests
from botocore.exceptions import ClientError

from configuration import HTTP_REQUEST_TIMEOUT, \
    BRANCH_ANALYTICS_URL_BASE, \
    data_sources, \
    aggregations
from debug import debug, dprint
from retry import retry
from utils import DynamoUtils

sendG = False  # Set to True to enable sending data to Apple, else a test run.
dashG = "-"
logger = logging.getLogger()
logger.setLevel(logging.INFO)

###### date and time parameters for bidding lookback ######
BIDDING_LOOKBACK = 7  # days #make this 2
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta

# FOR QA PURPOSES set these fields explicitly
#start_date = '2020-02-01'
#end_date = '2020-02-04'

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# ------------------------------------------------------------------------------
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

    clientsG = DynamoUtils.getClients(dynamodb)
    logger.info("In runBranchIntegration:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))

# ------------------------------------------------------------------------------
def getKeywordReportFromBranchHelper(url, payload, headers):
    dprint("url=%s." % url)
    dprint("json=%s." % json)
    return requests.post(url, json=payload, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)


# ------------------------------------------------------------------------------
@retry
def getKeywordReportFromBranch(branch_job, branch_key, branch_secret, aggregation):
    payload = {
        "branch_key": branch_key,
        "branch_secret": branch_secret,
        "start_date": str(start_date),
        "end_date": str(end_date),
        "data_source": branch_job,
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
        "granularity": "day", "aggregation": aggregation,
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

    response = getKeywordReportFromBranchHelper(url, payload, headers)
    logger.info("Response is %s." % response)

    return json.loads(response.text)


# ------------------------------------------------------------------------------
@debug
def process():
    for client in clientsG:
        branch_key = {}
        branch_secret = {}

        try:
            branch_key = client.branchIntegrationParameters["branch_key"]
            branch_secret = client.branchIntegrationParameters["branch_secret"]
            run_branch = True

        except KeyError as error:
            logger.info("runBranchIntegration:process:::no branch config skipping " + str(client.orgId))
            run_branch = False

        if run_branch:
            for data_source in data_sources.keys():
                # key field of db table (slice off the last character)
                data_source_key = data_source[:-1] + "_key"
                branch_job = data_sources.get(data_source)
                table = dynamodb.Table(data_source)

                if table:
                    logger.info("runBranchIntegration:process:::found table " + data_source)
                    branch_job_aggregations = aggregations[branch_job]

                    for aggregation in branch_job_aggregations:
                        data = {
                            aggregation: getKeywordReportFromBranch(branch_job, branch_key, branch_secret, aggregation)}

                        results = data[aggregation]['results']

                        if len(results) == 0:
                            logger.info("runBranchIntegration:process:::no results from " + branch_job)

                        for result in results:
                            if 'last_attributed_touch_data_tilde_campaign' in result["result"]:
                                if aggregation != "revenue":
                                    logger.debug(branch_job + ":::handle unique_count")
                                    timestamp = result["timestamp"].split('T')[0]
                                    campaign = str(result["result"]["last_attributed_touch_data_tilde_campaign"])
                                    campaign_id = str(result["result"]["last_attributed_touch_data_tilde_campaign_id"])
                                    ad_set_id = str(result["result"]["last_attributed_touch_data_tilde_ad_set_id"])
                                    # ad_set_name = str(result["result"]["last_attributed_touch_data_tilde_ad_set_name"])
                                    count = str(result["result"]["unique_count"])

                                    # event_key = campaign_id + dashG + ad_set_id + dashG + ad_set_name  # eg 197915189-197913017-search_match
                                    if 'last_attributed_touch_data_tilde_keyword' in result["result"]:
                                        keyword = str(result["result"]["last_attributed_touch_data_tilde_keyword"])
                                        event_key = campaign_id + dashG + ad_set_id + dashG + keyword.replace(" ", dashG)
                                    else:
                                        keyword = "n/a"
                                        event_key = campaign_id + dashG + ad_set_id

                                    # enable for local debugging
                                    # dprint("timestamp=%s." % timestamp)
                                    # dprint("campaign=%s." % campaign)
                                    # dprint("keyword=%s." % keyword)
                                    # dprint("count=%s." % count)
                                    # dprint("campaign_id=%s." % campaign_id)
                                    # dprint("event_key=%s." % event_key)

                                    try:
                                        response = table.put_item(
                                            Item={
                                                data_source_key: event_key,
                                                'timestamp': timestamp,
                                                'campaign': campaign,
                                                'campaign_id': campaign_id,
                                                'keyword': keyword,
                                                'ad_set_id': ad_set_id,
                                                # 'ad_set_name': ad_set_name,
                                                'count': count
                                            }
                                        )
                                    except ClientError as e:
                                        logger.warning("runBranchIntegration:process:::PutItem failed due to" + e.response['Error']['Message'])
                                    else:
                                        logger.debug("runBranchIntegration:process:::PutItem succeeded:")

                                else:
                                    # TODO refactor revenue to be order angostic, currently revenue must run after count
                                    logger.debug(branch_job + ":::handle revenue aggregation")
                                    timestamp = result["timestamp"].split('T')[0]
                                    campaign = str(result["result"]["last_attributed_touch_data_tilde_campaign"])
                                    campaign_id = str(result["result"]["last_attributed_touch_data_tilde_campaign_id"])
                                    ad_set_id = str(result["result"]["last_attributed_touch_data_tilde_ad_set_id"])
                                    # ad_set_name = str(result["result"]["last_attributed_touch_data_tilde_ad_set_name"])
                                    revenue = decimal.Decimal(result["result"]["revenue"])

                                    if 'last_attributed_touch_data_tilde_keyword' in result["result"]:
                                        keyword = str(result["result"]["last_attributed_touch_data_tilde_keyword"])
                                        event_key = campaign_id + dashG + ad_set_id + dashG + keyword.replace(" ", dashG)
                                    else:
                                        event_key = campaign_id + dashG + ad_set_id

                                    # enable for local debugging
                                    # dprint("timestamp=%s." % timestamp)
                                    # dprint("campaign=%s." % campaign)
                                    # dprint("keyword=%s." % keyword)
                                    # dprint("revenue=%d." % revenue)
                                    # dprint("campaign_id=%s." % campaign_id)
                                    # dprint("branch_commerce_event_key=%s." % event_key)

                                    try:
                                        response = table.update_item(
                                            Key={
                                                data_source_key: event_key,
                                                'timestamp': timestamp,
                                            },
                                            UpdateExpression='SET revenue = :val',
                                            ExpressionAttributeValues={
                                                ':val': revenue
                                            }
                                        )
                                    except ClientError as e:
                                        logger.warning("runBranchIntegration:process:::PutItem failed due to" + e.response['Error']['Message'])
                                        # enable for local debugging
                                        #print(json.dumps(response, indent=4, cls=DecimalEncoder))
                                    else:
                                        logger.debug("runBranchIntegration:process:::PutItem succeeded:")
                            else:
                                logger.info("runBranchIntegration:process:::Non keyword branch item found, skipping")
                else:
                    logger.info("runBranchIntegration:process:::issue connecting to " + data_source)

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
        'body': json.dumps('Run Branch Integration Complete')
    }
