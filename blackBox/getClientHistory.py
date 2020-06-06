import decimal
import json
import logging

import boto3

import debug
from utils import DynamoUtils

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
def initialize(env):
    global sendG
    global dynamodb

    if env == "lcl":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url='http://localhost:8000')
        # dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)
    elif env == "prod":
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)

# ------------------------------------------------------------------------------
def process(org_id):
    history = DynamoUtils.getClientHistory(dynamodb, org_id)

    return history


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    initialize('lcl')
    process('1056410')


def lambda_handler(event, context):
    initialize(event['env'])
    history = process(event['org_id'])

    return {
        'statusCode': 200,
        'body': json.dumps(history, cls=DecimalEncoder)
    }