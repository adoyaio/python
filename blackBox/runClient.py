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
    global logger
    
    sendG = LambdaUtils.getSendG(env)
    dynamoClient = LambdaUtils.getDynamoResource(env,dynamoEndpoint)
    # lambdaClient = LambdaUtils.getLambdaClient(env,awsEndpoint)
    # logger = LambdaUtils.getLogger(env)
    # logger.info("runBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s', emailTo='%s'" % (
    #     sendG, dynamoEndpoint, str(EMAIL_TO)))

def process(clientEvent, context):
    print("Run client:::clientEvent")
    print(str(clientEvent))
    print("Lambda Request ID:", context.aws_request_id)
    print("Lambda function ARN:", context.invoked_function_arn)
    
    
    # TODO check the jobdetails for which jobs will run
    appleIntegrationKeywordResponse = runAppleIntegrationKeyword.lambda_handler(clientEvent)
    # print(json.dumps(appleIntegrationKeywordResponse))

    runBranchIntegrationResponse = runBranchIntegration.lambda_handler(clientEvent)
    # print(json.dumps(runBranchIntegrationResponse))

    runClientDailyResponse = runClientDailyReport.lambda_handler(clientEvent)
    # print(json.dumps(runClientDailyResponse))

    runBidAdjusterResponse = runBidAdjuster.lambda_handler(clientEvent)
    # print(json.dumps(runBidAdjusterResponse))

    runAdgroupBidAdjusterResponse = runAdgroupBidAdjuster.lambda_handler(clientEvent)
    # print(json.dumps(runAdgroupBidAdjusterResponse))

    runKeywordAdderResponse = runKeywordAdder.lambda_handler(clientEvent)
    # print(json.dumps(runKeywordAdderResponse))

    return True

def lambda_handler(clientEvent, context):
    initialize(clientEvent['rootEvent']['env'], clientEvent['rootEvent']['dynamoEndpoint'], clientEvent['rootEvent']['emailToInternal'])
    process(clientEvent, context)
    return True
    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Run Client Complete')
    # }