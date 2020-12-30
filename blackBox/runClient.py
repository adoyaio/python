import json
import boto3
import runAppleIntegrationKeyword
import runBranchIntegration
import runBidAdjuster
import runAdgroupBidAdjuster
import runKeywordAdder
import runClientDailyReport
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils


def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global clientsG
    global dynamoResource
    global lambdaClient
    global EMAIL_TO
    global logger
    
    EMAIL_TO = emailToInternal
    sendG = LambdaUtils.getSendG(env)
    # clientsG = Client.getClients(dynamodb)
    dynamoClient = LambdaUtils.getDynamoResource(env,dynamoEndpoint)
    # lambdaClient = LambdaUtils.getLambdaClient(env,awsEndpoint)
    # logger = LambdaUtils.getLogger(env)
    # logger.info("runBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s', emailTo='%s'" % (
    #     sendG, dynamoEndpoint, str(EMAIL_TO)))

def process(clientEvent, context):
    print(str(clientEvent))
    
    # TODO check the jobdetails for which jobs will run
    appleIntegrationKeywordResponse = runAppleIntegrationKeyword.lambda_handler(clientEvent, context)
    print(json.dumps(appleIntegrationKeywordResponse))

    runBranchIntegrationResponse = runBranchIntegration.lambda_handler(clientEvent, context)
    print(json.dumps(runBranchIntegrationResponse))

    # invoke_response_branch = runBranchIntegration.lambda_handler(event, context)
    # print(json.dumps(invoke_response_branch))

    # invoke_response_dr = runClientDailyReport.lambda_handler(event, context)
    # print(json.dumps(invoke_response_dr))

    # invoke_response_rba = runBidAdjuster.lambda_handler(event, context)
    # print(json.dumps(invoke_response_rba))

    # invoke_response_raba = runAdgroupBidAdjuster.lambda_handler(event, context)
    # print(json.dumps(invoke_response_raba))

    # invoke_response_ka = runKeywordAdder.lambda_handler(event, context)
    # print(json.dumps(invoke_response_ka))

    # return True

def lambda_handler(clientEvent, context):
    initialize(clientEvent['rootEvent']['env'], clientEvent['rootEvent']['dynamoEndpoint'], clientEvent['rootEvent']['emailToInternal'])
    process(clientEvent, context)
    # terminate()
    print("JAMES TEST")
    return {
        'statusCode': 200,
        'body': json.dumps('Run Client Complete')
    }
