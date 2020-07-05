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
    print("Received context: " + str(context))
    print(str(context.client_context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    # TODO reevaluate this approach
    headers = event["headers"]
    host = "prod"
    if headers is not None:
        try:
            host = headers["Host"]
        except KeyError as error:
            host = "prod"

    if host == "localhost:3000":
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url='http://dynamodb:8000')
        print("using localhost db")
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        print("using prod db")

    client = DynamoUtils.getClient(dynamodb, org_id)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(client, cls=DecimalEncoder)
    }