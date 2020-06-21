from __future__ import print_function

import decimal
import sys

import boto3
import json

# sys.path.append("/blackBox/utils/")
from utils import DynamoUtils

# Helper class to convert a DynamoDB item to JSON.
# from utils import DynamoUtils
# from utils import DynamoUtils

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
    print("Received context: " + str(context))
    print(str(context.client_context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    # TODO set this via event
    env = "lcl"
    if env == "lcl":
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url='http://dynamodb:8000')
    elif env == "prod":
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    # handle logic for how to query dynamo
    query_by_time = False
    try:
        total_recs = queryStringParameters["total_recs"]
    except KeyError as error:
        query_by_time = True

    if query_by_time:
        start_date = queryStringParameters["start_date"]
        end_date = queryStringParameters["end_date"]
        history = DynamoUtils.getClientHistoryByTime(dynamodb, org_id, start_date, end_date)
    else:
        history = DynamoUtils.getClientHistoryNumRecs(dynamodb, org_id, total_recs)

    return {
        'statusCode': 200,
        'body': json.dumps(history, cls=DecimalEncoder)
    }
