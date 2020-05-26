import json
import runBranchBidAdjuster

def lambda_handler(event, context):
    invoke_response_bba = runBranchBidAdjuster.lambda_handler(event, context)
    print(json.dumps(invoke_response_bba))
    return True
