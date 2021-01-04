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

def initialize(env, dynamoEndpoint, lambdaEndpoint, emailToInternal):
    global sendG
    global clientsG
    global dynamoResource
    global lambdaClient
    global EMAIL_TO
    global logger
    
    EMAIL_TO = emailToInternal
    sendG = LambdaUtils.getSendG(env)
    dynamoResource = LambdaUtils.getDynamoResource(env,dynamoEndpoint)
    lambdaClient = LambdaUtils.getLambdaClient(env,lambdaEndpoint)
    clientsG = Client.getClients(dynamoResource)
    logger = LambdaUtils.getLogger(env)
    logger.info("runBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s', emailTo='%s'" % (
        sendG, dynamoEndpoint, str(EMAIL_TO)))

def process(event):
    for client in clientsG:
        print("JAMES TEST CLIENT ORG ID" + str(client.orgId))
        print("JAMES TEST CLIENT ORG NAME" + client.clientName)
        clientEvent = {}
        clientEvent['rootEvent'] = event
        clientEvent['orgDetails'] = json.dumps(client.__dict__,cls=DecimalEncoder)
        # TODO need to define list of jobs that will run with biweekly
        # clientEvent['jobDetails'] = []

        # invoke_response = lambdaClient.invoke(
        #     FunctionName='runClient',
        #     InvocationType='Event',
        #     LogType='None',
        #     Payload=json.dumps(clientEvent)
        # )

        # TODO need Event InvocationType for production
        invoke_response = lambdaClient.invoke(
            FunctionName='runClient',
            InvocationType='RequestResponse',
            Payload=json.dumps(clientEvent)
        )
        print(str(invoke_response))

    return True


def lambda_handler(event, context):
    initialize(event['env'], event['dynamoEndpoint'], event['lambdaEndpoint'], event['emailToInternal'])
    print("Lambda Request ID:", context.aws_request_id)
    print("Lambda function ARN:", context.invoked_function_arn)
    
    process(event)
    return {
        'statusCode': 200,
        'body': json.dumps('Run Adoya Complete')
    }
    # try: 
    #     process(event)
    # except:
    #     return {
    #         'statusCode': 400,
    #         'body': json.dumps('Run Adoya Failed')
    #     }
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Run Adoya Complete')
    # }
