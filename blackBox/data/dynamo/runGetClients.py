from __future__ import print_function  # Python 2/3 compatibility

import datetime
import decimal
import json
import logging
import pprint

import boto3

from debug import debug, dprint

sendG = False  # Set to True to enable sending data to Apple, else a test run.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

date = datetime.date
today = datetime.date.today()

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
        # dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)
    elif env == "prod":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)


# ------------------------------------------------------------------------------
@debug
def process():
    data = []
    for client in (dynamodb.Table('clients').scan()["Items"]):
        data.append(client["orgDetails"])
    dprint("%s" % pprint.pformat(data))
    with open('clients.' + str(today) + '.json', 'w') as outfile:
        json.dump(data, outfile, cls=DecimalEncoder, indent=4)

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