import decimal
import boto3
import json
import time
from utils import DynamoUtils, LambdaUtils, EmailUtils
from configuration import config

def postSupportItemHandler(event, context):  
    print('Loading postSupportItemHandler...')
    
    # parse event data
    body = json.loads(event["body"])
    payload = body["payload"]

    # init dynamo 
    send = LambdaUtils.getApiEnvironmentDetails(event).get('send')
 
    # get details from the payload including if its a request for proposal vs support ticket
    type = payload.get("type")

    # common values
    supportItem = json.loads(json.dumps(payload))
    subjectString = payload.get("subject", "error retrieving subject")
    clientEmailAddress = payload.get("username", "error retrieving ")
    description = payload.get("description", "")
    
    # contact us or support items
    clientEmailText = "Thank you for contacting Adoya. "
    clientEmailTextSupport = "We're checking into to your request and will respond within 24-48 hours."
    clientEmailTextOnboarding = "We've received and confirmed your invitation for Apple Search Ads API access. You are now ready to complete registration at https://adoya-app.io/registration (You may be required to re-authenticate)" \
    
    
    # if send:
    if True:
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

        if type == 'support' :
            # send email notification to client
            EmailUtils.sendTextEmail(
                clientEmailText + clientEmailTextSupport,
                subjectString, 
                [clientEmailAddress], 
                config.EMAIL_TO,
                config.EMAIL_FROM)

        if type == 'onboarding':
            # send email notification to client
            EmailUtils.sendTextEmail(
                clientEmailText + clientEmailTextOnboarding,
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