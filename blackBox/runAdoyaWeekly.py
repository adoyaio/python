
import json
import boto3
import sys
from Client import Client
from utils import LambdaUtils
from utils.DecimalEncoder import DecimalEncoder


def initialize(event):
    global sendG
    global clientsG
    global dynamoResource
    global lambdaClient
    global logger

    sendG = LambdaUtils.getSendG(
        event['env']
    )
    dynamoResource = LambdaUtils.getDynamoResource(
        event['env'],
        event['dynamoEndpoint']
    )

    lambdaClient = LambdaUtils.getLambdaClient(
        event['env'],
        event['lambdaEndpoint']
    )

    clientsG = Client.getClients(
        dynamoResource
    )

    logger = LambdaUtils.getLogger(
        event['env']
    )

def process(event):
    for client in clientsG:
        clientEvent = {}
        clientEvent['rootEvent'] = event
        clientEvent['orgDetails'] = client.toJSON()
        clientEvent['jobDetails'] = ['runBranchBidAdjuster']

        if event['env'] == 'prod':
            invoke_response = lambdaClient.invoke(
                FunctionName='runClient',
                InvocationType='Event',
                Payload=json.dumps(clientEvent)
            )
        else:
            invoke_response = lambdaClient.invoke(
                FunctionName='runClient',
                InvocationType='RequestResponse',
                Payload=json.dumps(clientEvent)
            )
        print(str(invoke_response))

    return True


def lambda_handler(event, context):
    initialize(event)
    try: 
        process(event)
    except:
        e = sys.exc_info()[0]
        return {
            'statusCode': 400,
            'body': json.dumps('Run Adoya Failed: ' + str(e))
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Adoya Complete')
    }