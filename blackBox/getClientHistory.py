import decimal
import sys
import boto3
import json
from utils import DynamoUtils, ApiUtils

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

print('Loading getClientHistory')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = ApiUtils.getDynamoHost(event)

    # handle logic for how to query dynamo
    query_by_time = False
    try:
        total_recs = queryStringParameters["total_recs"]
    except KeyError as error:
        query_by_time = True

    if query_by_time:
        start_date = queryStringParameters["start_date"]
        end_date = queryStringParameters["end_date"]
        history = DynamoUtils.getClientBranchHistoryByTime(dynamodb, org_id, start_date, end_date)
    else:
        history = DynamoUtils.getClientBranchHistoryNumRecs(dynamodb, org_id, total_recs)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(history, cls=DecimalEncoder)
    }
