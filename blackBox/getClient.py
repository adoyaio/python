import decimal
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

print('Loading getClientAPI')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received context: " + str(context.client_context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = ApiUtils.getDynamoHost(event)
    client = DynamoUtils.getClient(dynamodb, org_id)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(client, cls=DecimalEncoder)
    }