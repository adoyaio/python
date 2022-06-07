from Client import Client
import decimal
import boto3
import json
import time
from utils import DynamoUtils, LambdaUtils, EmailUtils
from configuration import config
from utils.DecimalEncoder import DecimalEncoder
import requests



def patchClientHandler(event, context):  
    print('Loading patchClientHandler...')
    
    # parse event data
    updateClientData: dict = json.loads(event["body"])
    updateApple = updateClientData.get('updateApple', False)
    updatedClient = updateClientData.get('client')
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    # init dynamo
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    send = LambdaUtils.getApiEnvironmentDetails(event).get('send')
    table = dynamodb.Table('clients')

    # get the current client model
    client: Client = DynamoUtils.getClient(dynamodb, org_id)

    # update client level data
    client.adgroupBidParameters = updatedClient.get('orgDetails').get('adgroupBidParameters')
    client.bidParameters = updatedClient.get('orgDetails').get('bidParameters')
    client.branchBidParameters = updatedClient.get('orgDetails').get('branchBidParameters')
    client.appleCampaigns = updatedClient.get('orgDetails').get('appleCampaigns')

    # write to dynamo
    updated = json.loads(client.toJSON(), parse_float=decimal.Decimal)
    table.put_item(
        Item = {
                'orgId': int(org_id),
                'orgDetails': updated
            }
    )

    # execute apple update if needed, only in live
    if updateApple and send:
        print("found auth values in client " + str(client.auth))
        authToken = LambdaUtils.getAuthToken(client.auth)
        headers = {
            "Authorization": "Bearer %s" % authToken, 
            "X-AP-Context": "orgId=%s" % org_id,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        for campaign in client.appleCampaigns:

            # handle campaign level
            url = config.APPLE_SEARCHADS_URL_BASE_V4 + (config.APPLE_CAMPAIGN_UPDATE_URL_TEMPLATE % campaign['campaignId'])
            payload = {
                "campaign": {
                     "budgetAmount": {
                        "amount": str(campaign['lifetimeBudget']),
                        "currency": str(client.currency)
                    },
                    "dailyBudgetAmount": {
                        "amount": str(campaign['dailyBudget']),
                        "currency": str(client.currency)
                    },
                    "status": str(campaign['status']),
                }
            }
            print("Apple URL is" + url)
            print("Headers are" + str(headers))
            print("Payload is '%s'." % payload)
            response = requests.put(
                url,
                json=payload,
                headers=headers,
                timeout=config.HTTP_REQUEST_TIMEOUT
            )
            print(str(response.text))
            print("The result of PUT campaign to Apple: %s" % response)


            # handle ag level

            # convert for apple 
            gender: list = campaign.get('gender', ['M','F'])
            if sorted(gender) == sorted(['M','F']):
                gender = None

            ad_group_url = config.APPLE_SEARCHADS_URL_BASE_V4 + (config.APPLE_ADGROUP_UPDATE_URL_TEMPLATE % (campaign['campaignId'], campaign['adGroupId']))
            ad_group_payload = {
                "name": str(campaign['adGroupName']),
                "targetingDimensions": {
                    "age": {
                        "included": [
                            {
                                "minAge": campaign.get('minAge', None),
                                "maxAge": campaign.get('maxAge', None)
                            }
                        ]
                    },
                    "gender": {
                        "included": gender
                    },
                    "country": None,
                    "adminArea": None,
                    "locality": None,
                    "deviceClass": None,
                    "daypart": None,
                    "appDownloaders": None
                }
            }
    
            print("Adgroup Apple URL is" + ad_group_url)
            print("Headers are" + str(headers))
            print("Adgroup Payload is '%s'." % ad_group_payload)
            response = requests.put(
                ad_group_url,
                json=ad_group_payload,
                headers=headers,
                timeout=config.HTTP_REQUEST_TIMEOUT
            )
            print(str(response.text))
            print("The result of PUT adgroup to Apple: %s" % response)


    if send:
        # send email notification, should only happen in live
        dateString = time.strftime("%m/%d/%Y")
        subjectString = "Client updated %s" % dateString
        EmailUtils.sendTextEmail(
            client.toJSON(),
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
        'body': json.dumps({})
    }


def postClientHandler(event, context):  
    print('Loading postClientHandler...')
    
    # parse event data
    body: dict = json.loads(event["body"])
    operation = body["operation"]
    payload = body["payload"]
    tableName = body["tableName"]
    updateApple = body.get('updateApple', False)

    # parse query string para
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    # init dynamo 
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    send = LambdaUtils.getApiEnvironmentDetails(event).get('send')
    table = dynamodb.Table(tableName)

    # operation to callback mapping TODO error on invalid
    operations = {
        'create': lambda x: table.put_item(
            Item={**x}
        )
    }
    
    # parse decimals to float for email
    clientDict = json.loads(json.dumps(payload), parse_float=decimal.Decimal)
    
    # execute callback(client)
    response = operations[operation](clientDict)

    # apple part
    client: Client = DynamoUtils.getClient(dynamodb, org_id)

    # execute apple update if needed
    if updateApple:
        print("found auth values in client " + str(client.auth))
        authToken = LambdaUtils.getAuthToken(client.auth)
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + (config.APPLE_CAMPAIGN_UPDATE_URL_TEMPLATE % newCampaignValues['campaignId'])
        headers = {
            "Authorization": "Bearer %s" % authToken, 
            "X-AP-Context": "orgId=%s" % org_id,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        for campaign in client.appleCampaigns:
            payload = {
                "campaign": {
                     "budgetAmount": {
                        "amount": str(campaign['lifetimeBudget']),
                        "currency": str(client.currency)
                    },
                    "dailyBudgetAmount": {
                        "amount": str(campaign['dailyBudget']),
                        "currency": str(client.currency)
                    },
                    "status": str(campaign['status']),
                }
            }
            print("Apple URL is" + url)
            print("Headers are" + str(headers))
            print("Payload is '%s'." % payload)
            response = requests.put(
                url,
                json=payload,
                headers=headers,
                timeout=config.HTTP_REQUEST_TIMEOUT
            )
            print(str(response.text))
            print("The result of PUT campaign to Apple: %s" % response)

    if send:
        # send email notification, should only happen in live
        dateString = time.strftime("%m/%d/%Y")
        subjectString = "Client updated %s" % dateString
        EmailUtils.sendTextEmail(
            json.dumps(
                clientDict,
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

    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    send = LambdaUtils.getApiEnvironmentDetails(event).get('send')
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

    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    client = DynamoUtils.getClient(dynamodb, org_id)
    clientJSON = client.toJSON()

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': clientJSON
    }

def getClientCostHistoryHandler(event, context):
    print('Loading getClientCostHistoryHandler....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')

    offset = {
        "org_id": queryStringParameters.get("offsetOrgId"),
        "timestamp": queryStringParameters.get("offsetDate"),
    }

    total_recs = queryStringParameters.get("total_recs", "100")
    start_date = queryStringParameters.get("start_date", "all")
    end_date = queryStringParameters.get("end_date", "all")

    response = DynamoUtils.getClientBranchHistoryByTime(
        dynamodb,
        org_id,
        total_recs,
        offset,
        start_date,
        end_date
    )

    # query_by_time = False
    # try:
    #     total_recs = queryStringParameters["total_recs"]
    # except KeyError as error:
    #     query_by_time = True

    # if query_by_time:
    #     start_date = queryStringParameters["start_date"]
    #     end_date = queryStringParameters["end_date"]
    #     history = DynamoUtils.getClientBranchHistoryByTime(dynamodb, org_id, start_date, end_date)
    # else:
    #     history = DynamoUtils.getClientBranchHistory(dynamodb, org_id, total_recs)

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(response, cls=DecimalEncoder)
    }

# def getClientCampaignHistoryHandler(event, context):
#     print('Loading getCampaignCostHistoryHandler....')
#     print("Received event: " + json.dumps(event, indent=2))
#     print("Received context: " + str(context))
#     queryStringParameters = event["queryStringParameters"]
#     campaign_id = str(queryStringParameters["campaign_id"])

#     dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')

#     offset = {
#         "campaign_id": queryStringParameters.get("offsetCampaignId"),
#         "timestamp": queryStringParameters.get("offsetDate"),
#     }

#     total_recs = queryStringParameters.get("total_recs", "100")
#     start_date = queryStringParameters.get("start_date", "all")
#     end_date = queryStringParameters.get("end_date", "all")

#     response = DynamoUtils.getCampaignBranchHistoryByTime(
#         dynamodb,
#         campaign_id,
#         total_recs,
#         offset,
#         start_date,
#         end_date
#     )

#     return {
#         'statusCode': 200,
#         'headers': {
#             'Access-Control-Allow-Origin': '*',
#             'Access-Control-Allow-Methods': 'GET',
#             'Access-Control-Allow-Headers': 'x-api-key'
#         },
#         'body': json.dumps(response, cls=DecimalEncoder)
#     }

def getClientCampaignHistoryHandler(event, context):
    print('Loading getCampaignCostHistoryHandler....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    multiValueQueryStringParameters = event["multiValueQueryStringParameters"]
    # campaign_id = str(queryStringParameters["campaign_id"])
    org_id = str(queryStringParameters["org_id"])
    campaign_ids = multiValueQueryStringParameters["campaign_id"]

    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')

    offset = {
        "campaign_id": queryStringParameters.get("offsetCampaignId"),
        "timestamp": queryStringParameters.get("offsetDate"),
        "org_id": org_id,
    }

    total_recs = queryStringParameters.get("total_recs", "100")
    start_date = queryStringParameters.get("start_date", "all")
    end_date = queryStringParameters.get("end_date", "all")

    response = DynamoUtils.getCampaignBranchHistoryByTime(
        dynamodb,
        campaign_ids,
        org_id,
        total_recs,
        offset,
        start_date,
        end_date
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


def getClientKeywordHistoryHandler(event, context):
    print('Loading getClientKeywordHistoryHandler....')
    print("Received event: " + json.dumps(event, indent=2))
    
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')

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
