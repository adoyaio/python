from __future__ import print_function  # Python 2/3 compatibility
import logging
import decimal
import boto3
from collections import defaultdict
import datetime
import json
import pandas as pd
import pprint
import requests
import sys
import time

from botocore.exceptions import ClientError

from Client import CLIENTS
from configuration import EMAIL_FROM, \
    APPLE_UPDATE_POSITIVE_KEYWORDS_URL, \
    APPLE_KEYWORD_REPORTING_URL_TEMPLATE, \
    TOTAL_COST_PER_INSTALL_LOOKBACK, \
    HTTP_REQUEST_TIMEOUT, \
    BRANCH_ANALYTICS_URL_BASE, \
    data_sources, \
    aggregations
from debug import debug, dprint

sendG = False  # Set to True to enable sending data to Apple, else a test run.
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
def initialize(env, dynamoEndpoint):
    global sendG
    global dynamodb

    if env == "lcl":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
        logger.setLevel(logging.INFO)
    elif env == "prod":
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)  # TODO reduce AWS logging in production
        # debug.disableDebug() TODO disable debug wrappers in production
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)

    logger.info("In runMigrationV1:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


# ------------------------------------------------------------------------------
@debug
def process():

    for client in CLIENTS:
        print("Print CPI history for: " + str(client.clientName))
        print(client.orgId)
        history = client.getHistory(dynamodb);
        print(str(history));

# ------------------------------------------------------------------------------
@debug
def terminate():
    pass


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    initialize('lcl', 'http://localhost:8000')
    process()
    terminate()

    # initialize('prod', 'http://localhost:8000')
    # process()
    # terminate()