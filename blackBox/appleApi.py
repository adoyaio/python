import decimal
import boto3
import json
import time
from utils.debug import debug, dprint
from utils import DynamoUtils, ApiUtils, EmailUtils, LambdaUtils
from configuration import config
from utils.DecimalEncoder import DecimalEncoder
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from Client import Client
import requests


# def getAppleKeys(event, context):
#     print('Loading getAppleKeys....')
#     print("Received event: " + json.dumps(event, indent=2))
#     print("Received context: " + str(context))
#     print("Received context: " + str(context.client_context))
#     # queryStringParameters = event["queryStringParameters"]
#     # org_id = queryStringParameters["org_id"]

#     # dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
#     # client = DynamoUtils.getClient(dynamodb, org_id)
#     private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
#     public_key = private_key.public_key()
#     # serializing into PEM
#     ec_key = private_key.private_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.OpenSSH, encryption_algorithm=ec.SECP256R1)
#     ec_pem = public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.OpenSSH)
#     print(ec_pem.decode())
#     print(ec_key.decode())

#     returnKey = ec_key.decode()
#     returnPem = ec_pem.decode()

#     return {
#         'statusCode': 200,
#         'headers': {
#             'Access-Control-Allow-Origin': '*',
#             'Access-Control-Allow-Methods': 'GET',
#             'Access-Control-Allow-Headers': 'x-api-key'
#         },
#         'body': { 'privateKey': returnKey, 'publicKey': returnPem }
#     }

def getAppleApps(event, context):
    print('Loading getAppleApps....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received context: " + str(context.client_context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
    client = DynamoUtils.getClient(dynamodb, org_id)

    # handle auth token
    print(str(client))
    auth = client[0].get('orgDetails').get('auth', None)
    if auth is not None:
        print("found auth values in client " + str(auth))
        authToken = LambdaUtils.getAuthToken(auth)
        url = config.APPLE_SEARCHADS_URL_BASE_V3 + config.APPLE_GET_APPS_URL
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % org_id}
        dprint("\nURL is '%s'." % url)
        dprint ("\nHeaders are %s." % headers)      
        response = requests.get(
            url,
            headers=headers,
            timeout=config.HTTP_REQUEST_TIMEOUT
        )

        print(str(response.text))
        
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(json.loads(response.text))
    }