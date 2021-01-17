import logging
import json
import boto3
import runAppleIntegrationKeyword
import runBranchIntegration
import runBidAdjuster
import runAdgroupBidAdjuster
import runKeywordAdder
import runClientDailyReport
from Client import Client
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
from utils.DecimalEncoder import DecimalEncoder

def initialize(event):
    global sendG
    global clientsG
    global dynamoResource
    global lambdaClient
    global logger

    print(str(event))
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
        clientEvent['orgDetails'] = json.dumps(client.__dict__,cls=DecimalEncoder)
        clientEvent['jobDetails'] = ['runAppleIntegrationKeyword', 'runBranchIntegration', 'runClientDailyReport', 'runBidAdjuster', 'runAdgroupBidAdjuster','runKeywordAdder']

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
    # print("Lambda Request ID:", context.aws_request_id)
    # print("Lambda function ARN:", context.invoked_function_arn)
    
    try: 
        process(event)
    except:
        return {
            'statusCode': 400,
            'body': json.dumps('Run Adoya Failed')
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Adoya Complete')
    }
