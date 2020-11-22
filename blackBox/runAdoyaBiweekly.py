import json
import boto3
import runAppleIntegrationKeyword
import runBranchIntegration
import runBidAdjuster
import runAdgroupBidAdjuster
import runKeywordAdder
import runClientDailyReport


def lambda_handler(event, context):
    # invoke_response_branch = runAppleIntegration.lambda_handler(event, context)
    # print(json.dumps(invoke_response_branch))

    invoke_response_branch = runAppleIntegrationKeyword.lambda_handler(event, context)
    print(json.dumps(invoke_response_branch))

    invoke_response_branch = runBranchIntegration.lambda_handler(event, context)
    print(json.dumps(invoke_response_branch))

    invoke_response_dr = runClientDailyReport.lambda_handler(event, context)
    print(json.dumps(invoke_response_dr))

    invoke_response_rba = runBidAdjuster.lambda_handler(event, context)
    print(json.dumps(invoke_response_rba))

    invoke_response_raba = runAdgroupBidAdjuster.lambda_handler(event, context)
    print(json.dumps(invoke_response_raba))

    invoke_response_ka = runKeywordAdder.lambda_handler(event, context)
    print(json.dumps(invoke_response_ka))

    return True

    # lambda_client = boto3.client('lambda', region_name="us-east-1")
    # invoke_response = lambda_client.invoke(
    #         FunctionName='runBranchIntegration',
    #         InvocationType='Event',
    #         LogType='None',
    #         Payload=json.dumps(event)
    # )
