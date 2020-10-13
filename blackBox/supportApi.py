import decimal
import boto3
import json
import time
from utils import DynamoUtils, ApiUtils, EmailUtils
from configuration import config

def postSupportItemHandler(event, context):  
    print('Loading postSupportItemHandler...')
    
    # parse event data
    body = json.loads(event["body"])
    payload = body["payload"]

    # init dynamo 
    send = ApiUtils.getDynamoHost(event).get('send')
 
    # parse decimals to float for email
    subjectString = payload.get("subject", "error retrieving subject")
    clientEmailAddress = payload.get("username", "info@adoya.io")
    clientEmailText = "Thank you for contacting Adoya support, we're checking into it!"
    supportItem = json.loads(json.dumps(payload))

    
    if send:

        # send email notification internal, should only happen in live
        EmailUtils.sendTextEmail(
            json.dumps(
                supportItem,  
                indent=2
            ), 
            subjectString, 
            config.EMAIL_TO, 
            [],
            config.EMAIL_FROM)

        # send email notification to client
        EmailUtils.sendTextEmail(
            clientEmailText,
            subjectString, 
            [clientEmailAddress], 
            config.EMAIL_TO,
            config.EMAIL_FROM)

     # return
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps("support ticket created")
    }