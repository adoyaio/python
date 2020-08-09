import decimal
import time
import boto3
import json
from utils import DynamoUtils, ApiUtils, EmailUtils
from configuration import config

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

print('Loading postClient')

def lambda_handler(event, context):
    '''Provide an event that contains the following keys:
      - operation: one of the operations in the operations dict below
      - tableName: required for operations that interact with DynamoDB
      - payload: a parameter to pass to the operation being performed
    '''
    body = json.loads(event["body"])
    operation = body["operation"]
    payload = body["payload"]
    tableName = body["tableName"]
    dynamodb = ApiUtils.getDynamoHost(event)
    dynamo = dynamodb.Table(tableName)
    operations = {
        'create': lambda x: dynamo.put_item(
            Item={**x}
        ),
        'read': lambda x: dynamo.get_item(**x),
        'update': lambda x: dynamo.update_item(**x),
        'delete': lambda x: dynamo.delete_item(**x),
        'list': lambda x: dynamo.scan(**x),
        'echo': lambda x: x,
        'ping': lambda x: 'pong'
    }

    clients = json.loads(json.dumps(payload), parse_float=decimal.Decimal)
    payload = clients

    # send internal email on client update details
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString = "Client updated %s" % dateString
    EmailUtils.sendTextEmail(json.dumps(payload, cls=DecimalEncoder, indent=2), subjectString, config.EMAIL_TO, [], config.EMAIL_FROM)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(operations[operation](payload), cls=DecimalEncoder)
    }
