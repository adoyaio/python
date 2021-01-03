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

    runClientDailyResponse = runClientDailyReport.lambda_handler(clientEvent, context)
    print(json.dumps(runClientDailyResponse))

    runBidAdjusterResponse = runBidAdjuster.lambda_handler(clientEvent, context)
    print(json.dumps(runBidAdjusterResponse))

    runAdgroupBidAdjusterResponse = runAdgroupBidAdjuster.lambda_handler(clientEvent, context)
    print(json.dumps(runAdgroupBidAdjusterResponse))

    runKeywordAdderResponse = runKeywordAdder.lambda_handler(clientEvent, context)
    print(json.dumps(runKeywordAdderResponse))

    return True

def lambda_handler(clientEvent, context):
    initialize(clientEvent['rootEvent']['env'], clientEvent['rootEvent']['dynamoEndpoint'], clientEvent['rootEvent']['emailToInternal'])
    
    try: 
        process(clientEvent, context)
    except:
        return {
            'statusCode': 400,
            'body': json.dumps('Run Client Failed')
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Client Complete')
    }
