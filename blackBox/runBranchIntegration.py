import datetime
import decimal
import json
import logging
import boto3
import requests
import time
from Client import Client
from configuration import config
from utils import DynamoUtils, LambdaUtils, EmailUtils
from utils.debug import debug, dprint
from utils.retry import retry
from botocore.exceptions import ClientError # eliminate inexact and rounding errors
from boto3.dynamodb.types import DYNAMODB_CONTEXT 
from botocore.exceptions import ClientError
DYNAMODB_CONTEXT.traps[decimal.Inexact] = 0
DYNAMODB_CONTEXT.traps[decimal.Rounded] = 0

dashG = "-"
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
    logger.info("runBranchIntegration:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))

@retry
def getKeywordReportFromBranchHelper(url, payload, headers):
    dprint("url=%s." % url)
    dprint("json=%s." % json)
    return requests.post(url, json=payload, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

@retry
def getKeywordReportFromBranch(client, branch_job, branch_key, branch_secret, aggregation):
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

    url: str = config.BRANCH_ANALYTICS_URL_BASE
    headers = {"Content-Type": "application/json"}
    logger.info("URL is '%s'." % url)
    logger.info("Payload is '%s'." % payload)
    logger.info("Headers are %s." % headers)
    response = getKeywordReportFromBranchHelper(url, payload, headers)
    logger.info("Response is %s." % response)
    
    # TODO extract to utils
    if response.status_code != 200:
        email = "client id:%d \n url:%s \n payload:%s \n response:%s" % (client.orgId, url, payload, response)
        date = time.strftime("%m/%d/%Y")
        subject ="%s - %d ERROR in runBranchIntegration for %s" % (date, response.status_code, client.clientName)
        logger.warn(email)
        logger.error(subject)
        if sendG:
            EmailUtils.sendTextEmail(email, subject, EMAIL_TO, [], config.EMAIL_FROM)
        
        return False

    return json.loads(response.text)


@debug
def process():
    for client in clientsG:
        try:
            branch_key = client.branchIntegrationParameters["branch_key"]
            branch_secret = client.branchIntegrationParameters["branch_secret"]
        except KeyError as error:
            logger.info("runBranchIntegration:::no branch config skipping" + str(client.orgId))
            continue

        for data_source in config.DATA_SOURCES.keys():    
            data_source_key = data_source[:-1] + "_key" # key field of db table, slice off the last character
            branch_job = config.DATA_SOURCES.get(data_source)
            table = dynamodb.Table(data_source)
            if not table:
                logger.info("runBranchIntegration:process:::issue connecting to:::" + data_source)
                continue
            
            logger.info("runBranchIntegration:::found table!" + data_source)
            branch_job_aggregations = config.AGGREGATIONS[branch_job]
            for aggregation in branch_job_aggregations:
                response = getKeywordReportFromBranch(
                    client,
                    branch_job, 
                    branch_key, 
                    branch_secret, 
                    aggregation
                )
                
                if not response:
                    logger.info("runBranchIntegration:process:::no results from:::" + branch_job)
                    continue
                data = { aggregation: response }
                results = data[aggregation]['results']
                for result in results:
                    if not 'last_attributed_touch_data_tilde_campaign' in result["result"]:
                        logger.info("runBranchIntegration:process:::Non keyword branch item found, skipping")
                        continue
                    
                    if aggregation != "revenue":
                        logger.debug(branch_job + ":::handle unique_count")
                        timestamp = result["timestamp"].split('T')[0]
                        campaign = str(result["result"]["last_attributed_touch_data_tilde_campaign"])
                        campaign_id = str(result["result"]["last_attributed_touch_data_tilde_campaign_id"])
                        ad_set_id = str(result["result"]["last_attributed_touch_data_tilde_ad_set_id"])
                        # ad_set_name = str(result["result"]["last_attributed_touch_data_tilde_ad_set_name"])
                        count = str(result["result"]["unique_count"])
                        # event_key === campaign_id + dashG + ad_set_id + dashG + ad_set_name  # eg 197915189-197913017-search_match
                        if 'last_attributed_touch_data_tilde_keyword' in result["result"]:
                            keyword = str(result["result"]["last_attributed_touch_data_tilde_keyword"])
                            event_key = campaign_id + dashG + ad_set_id + dashG + keyword.replace(" ", dashG)
                        else:
                            keyword = "n/a"
                            event_key = campaign_id + dashG + ad_set_id
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
                                    'count': count,
                                    'org_id': str(client.orgId)
                                }
                            )
                        except ClientError as e:
                            logger.warning("runBranchIntegration:process:::PutItem failed due to" + e.response['Error']['Message'])
                        else:
                            logger.debug("runBranchIntegration:process:::PutItem succeeded:")
                    else:
                        # NOTE currently revenue must run after count
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
                            print(json.dumps(response, indent=4, cls=DecimalEncoder))
                        else:
                            logger.debug("runBranchIntegration:process:::PutItem succeeded")
                    

@debug
def terminate():
    pass


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
