from __future__ import print_function

import decimal

import boto3
import json


# Helper class to convert a DynamoDB item to JSON.
from utils import DynamoUtils


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

print('Loading function')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # queryStringParameters = json.loads(event["queryStringParameters"])
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    start_date = queryStringParameters["start_date"]
    end_date = queryStringParameters["end_date"]

    # TODO set this via event or context
    env = "lcl"
    if env == "lcl":
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url='http://dynamodb:8000')
    elif env == "prod":
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    history = DynamoUtils.getClientHistoryByTime(dynamodb, org_id, start_date, end_date)

    return {
        'statusCode': 200,
        'body': json.dumps(history, cls=DecimalEncoder)
    }