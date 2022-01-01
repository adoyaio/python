import json
import boto3
import runAppleIntegrationKeyword
import runBranchIntegration
import runBidAdjuster
import runAdgroupBidAdjuster
import runKeywordAdder
import runAdGroupBidAdjusterPoorPerformer
import runBidAdjusterPoorPerformer
import runClientDailyReport
import runBranchBidAdjuster
import runCampaignSync
import runAppleIntegrationCampaign
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
from Client import Client


def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global clientsG
    global dynamoResource
    global lambdaClient
    global logger
    
    sendG = LambdaUtils.getSendG(env)
    dynamoClient = LambdaUtils.getDynamoResource(env,dynamoEndpoint)

def process(clientEvent, context):
    print("Lambda Request ID:", context.aws_request_id)
    print("Lambda function ARN:", context.invoked_function_arn)
    
    # get the list of jobs to run from the clientEvent
    jobDetails = clientEvent['jobDetails']

    # get auth token and add to the clientEvent, default to None
    clientEvent['authToken'] = None
    
    client = Client.buildFromDictionary(
        json.loads(
            clientEvent['orgDetails']
        )
    )
    if client.auth is not None:
        authToken = LambdaUtils.getAuthToken(client.auth)
        
        # add accesToken to the clientEvent
        clientEvent["authToken"] = authToken

    if 'runAppleIntegrationKeyword' in jobDetails:
        appleIntegrationKeywordResponse = runAppleIntegrationKeyword.lambda_handler(clientEvent)
        print(json.dumps(appleIntegrationKeywordResponse))

    if 'runBranchIntegration' in jobDetails:
        runBranchIntegrationResponse = runBranchIntegration.lambda_handler(clientEvent)
        print(json.dumps(runBranchIntegrationResponse))

    if 'runAppleIntegrationCampaign' in jobDetails:
        runAppleIntegrationCampaignResponse = runBranchIntegration.lambda_handler(clientEvent)
        print(json.dumps(runAppleIntegrationCampaignResponse))

    if 'runClientDailyReport' in jobDetails:
        runClientDailyResponse = runClientDailyReport.lambda_handler(clientEvent)
        print(json.dumps(runClientDailyResponse))

    if 'runBidAdjuster' in jobDetails:
        runBidAdjusterResponse = runBidAdjuster.lambda_handler(clientEvent)
        print(json.dumps(runBidAdjusterResponse))

    if 'runAdgroupBidAdjuster' in jobDetails:
        runAdgroupBidAdjusterResponse = runAdgroupBidAdjuster.lambda_handler(clientEvent)
        print(json.dumps(runAdgroupBidAdjusterResponse))

    if 'runBidAdjusterPoorPerformer' in jobDetails:
        runBidAdjusterPoorPerformerResponse = runBidAdjusterPoorPerformer.lambda_handler(clientEvent)
        print(json.dumps(runBidAdjusterPoorPerformerResponse))

    if 'runAdGroupBidAdjusterPoorPerformer' in jobDetails:
        runAdGroupBidAdjusterPoorPerformerResponse = runAdGroupBidAdjusterPoorPerformer.lambda_handler(clientEvent)
        print(json.dumps(runAdGroupBidAdjusterPoorPerformerResponse))

    if 'runKeywordAdder' in jobDetails:
        runKeywordAdderResponse = runKeywordAdder.lambda_handler(clientEvent)
        print(json.dumps(runKeywordAdderResponse))

    if 'runBranchBidAdjuster' in jobDetails:
        runBranchBidAdjusterResponse = runBranchBidAdjuster.lambda_handler(clientEvent)
        print(json.dumps(runBranchBidAdjusterResponse))

    if 'runCampaignSync' in jobDetails:
        runCampaignSyncResponse = runCampaignSync.lambda_handler(clientEvent)
        print(json.dumps(runCampaignSyncResponse))


    return True

def lambda_handler(clientEvent, context):
    initialize(clientEvent['rootEvent']['env'], clientEvent['rootEvent']['dynamoEndpoint'], clientEvent['rootEvent']['emailToInternal'])
    process(clientEvent, context)

    return {
        'statusCode': 200,
        'body': json.dumps('Run Client Complete')
    }