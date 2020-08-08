from __future__ import print_function

import decimal

import boto3
import json

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

print('Loading postAdmin')

def lambda_handler(event, context):
    '''Provide an event that contains the following keys:

      - operation: one of the operations in the operations dict below
      - tableName: required for operations that interact with DynamoDB
      - payload: a parameter to pass to the operation being performed
    '''
    # print("Received event: " + json.dumps(event, indent=2))
    body = json.loads(event["body"])
    operation = body["operation"]
    payload = body["payload"]
    tableName = body["tableName"]

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

    if(operation == 'create'):
        clients = json.loads(json.dumps(payload), parse_float=decimal.Decimal)
        payload = clients


    return {
        'statusCode': 200,
        'body': json.dumps(operations[operation](payload), cls=DecimalEncoder)
    }