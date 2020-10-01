import decimal
import boto3
import json
import time
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

def postClientHandler(event, context):  
    print('Loading postClientHandler...')
    
    # parse event data
    body = json.loads(event["body"])
    operation = body["operation"]
    payload = body["payload"]
    tableName = body["tableName"]

    # init dynamo 
    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
    send = ApiUtils.getDynamoHost(event).get('send')
    table = dynamodb.Table(tableName)

    # operation to callback mapping TODO error on invalid
    operations = {
        'create': lambda x: table.put_item(
            Item={**x}
        )
    }
    
    # parse decimals to float for email
    client = json.loads(json.dumps(payload), parse_float=decimal.Decimal)
    
    # execute callback(client)
    response = operations[operation](client)

    if send:
        # send email notification, should only happen in live
        dateString = time.strftime("%m/%d/%Y")
        subjectString = "Client updated %s" % dateString
        EmailUtils.sendTextEmail(
            json.dumps(
                client, 
                cls=DecimalEncoder, 
                indent=2
            ), 
            subjectString, 
            config.EMAIL_TO, 
            [],
            config.EMAIL_FROM)

     # return parsed json from dynamo
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(response, cls=DecimalEncoder)
    }

def postClientAdminHandler(event, context):
    print('Loading postClientAdminHandler....')
    body = json.loads(event["body"])
    operation = body["operation"]
    payload = body["payload"]
    tableName = body["tableName"]

    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
    send = ApiUtils.getDynamoHost(event).get('send')
    table = dynamodb.Table(tableName)

    operations = {
        'create': lambda x: table.put_item(
            Item={**x}
        ),
        'read': lambda x: table.get_item(**x),
        'update': lambda x: table.update_item(**x),
        'delete': lambda x: table.delete_item(**x),
        'list': lambda x: table.scan(**x),
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


def getClientHandler(event, context):
    print('Loading getClientHandler....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received context: " + str(context.client_context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
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

def getClientCostHistoryHandler(event, context):
    print('Loading getClientCostHistoryHandler....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')

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
        history = DynamoUtils.getClientBranchHistory(dynamodb, org_id, total_recs)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(history, cls=DecimalEncoder)
    }


def getClientKeywordHistoryHandler(event, context):
    print('Loading getClientKeywordHistoryHandler....')
    print("Received event: " + json.dumps(event, indent=2))
    
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')

    offset = {
        "org_id": queryStringParameters.get("offsetOrgId"),
        "date": queryStringParameters.get("offsetDate"),
        "keyword_id": queryStringParameters.get("offsetKeywordId")
    }

    total_recs = queryStringParameters.get("total_recs", "100")
    start_date = queryStringParameters.get("start_date", "all")
    end_date = queryStringParameters.get("end_date", "all")
    matchType = queryStringParameters.get("matchType",'all')
    keywordStatus = queryStringParameters.get("keywordStatus",'all')

    response = DynamoUtils.getClientKeywordHistory(
        dynamodb, 
        org_id, 
        total_recs, 
        offset,
        end_date,
        start_date,
        matchType,
        keywordStatus
    )

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(response, cls=DecimalEncoder)
    }
