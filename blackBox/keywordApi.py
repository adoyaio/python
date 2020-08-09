import decimal
import boto3
import json
import time
from utils import DynamoUtils, ApiUtils, EmailUtils
from configuration import config


def getKeywordHistoryHandler(event, context):
    print('Loading getKeywordHandler...')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received context: " + str(context.client_context))
    
    # parse event for querystring
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    
    # configure dynamo
    dynamodb = ApiUtils.getDynamoHost(event)

    # TODO wireup
    # keywords = DynamoUtils.getKeywords(dynamodb, org_id)

    # return 
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(client, cls=DecimalEncoder)
    }