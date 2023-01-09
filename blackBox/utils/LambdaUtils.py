import boto3
import botocore.config
import logging
import json
import decimal
import jwt
import requests
import datetime as dt
from configuration import config
from utils.DecimalEncoder import DecimalEncoder
from utils.debug import debug, dprint
from Client import Client

# TODO reevaluate this approach
def getApiEnvironmentDetails(event):
    headers = event["headers"]
    host = "prod"
    if headers is not None:
        try:
            host = headers["Host"]
        except KeyError as error:
            host = "prod"

    if host == "localhost:3000" or host == "127.0.0.1:3000":
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url='http://dynamodb:8000')
        returnValue = { 'dynamodb': dynamodb, 'send': False}
        print("LambdaUtils.getDynamoHost:::LOCALHOST") 
    else:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        returnValue = { 'dynamodb': dynamodb, 'send': True }
        print("LambdaUtils.getDynamoHost:::PROD")

    return returnValue


def getSendG(env):
    if env == "lcl":
        return False
    elif env == "stage": # stage updates
        return True
    elif env == "prod":
        return True
    else:
        return False

def getEmailClientsG(env):
    if env == "lcl":
        return False
    elif env == "stage": # stage doesnt email
        return False
    elif env == "prod":
        return True
    else:
        return False

def getDynamoResource(env, endpoint):
    if env == "lcl":
        return boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=endpoint)
    elif env == "stage":
        return boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=endpoint)
    elif env == "prod":
        return boto3.resource('dynamodb', region_name='us-east-1')
    else:
        return boto3.resource('dynamodb', region_name='us-east-1')

def getLambdaClient(env, endpoint):
    cfg = botocore.config.Config(retries={'max_attempts': 0},read_timeout=900, connect_timeout=900, region_name="us-east-1")
    if env == "lcl":
        return boto3.client('lambda', region_name='us-east-1', endpoint_url=endpoint, config=cfg)
    elif env == "stage":
        return boto3.client('lambda', region_name='us-east-1', endpoint_url=endpoint, config=cfg)
    elif env == "prod":
        return boto3.client('lambda', region_name='us-east-1', config=cfg)
    else:
        return boto3.client('lambda', region_name='us-east-1', config=cfg)
    
def getLogger(env):
    logger = logging.getLogger()
    if env == "lcl":
        logger.setLevel(logging.INFO)
        return logger
    elif env == "prod":
        logger.setLevel(logging.INFO)
        return logger
    else:
        logger.setLevel(logging.INFO)
        return logger

def getBidParamsForJob(client: Client, campaign, job):
    if job == "bidAdjuster":
        params = {}
        params.update(client.bidParameters)
        params.update(campaign.get('bidParameters',[]))
        return params

    if job == "branchBidAdjuster":
        params = {}
        params.update(client.branchBidParameters)
        params.update(campaign.get('branchBidParameters',[]))
        return params


# build a mock clientEvent
def getClientForLocalRun(orgId, emailToInternal):
    clientEvent = {}
    clientEvent['rootEvent'] = {
        "env": "lcl",
        "dynamoEndpoint": "http://localhost:8000",
        "lambdaEndpoint": "http://host.docker.internal:3001",
        "emailToInternal": emailToInternal
    }
    # uncomment to run for prod
    # clientEvent['rootEvent'] = {
    #   "env": "prod",
    #   "dynamoEndpoint": "",
    #   "lambdaEndpoint": "",
    #     "emailToInternal": emailToInternal
    # }
    with open("./data/dynamo/clients.json") as json_file:
        clients = json.load(json_file)
        clientDict = next(item for item in clients if item["orgId"] == str(orgId))
        
        client = Client.buildFromDictionary(
            clientDict['orgDetails']
        )
    # serialize to json for mock lambda event, 
    clientEvent['orgDetails'] = client.toJSON()

    # handle auth token
    if client.auth is None:
        clientEvent['authToken'] = None
        return clientEvent
        
    authToken = getAuthToken(client.auth, client.orgId)
    clientEvent['authToken'] = authToken
    return clientEvent


# gets an oauth token from appleid.apple.com
def getAuthToken(auth, orgId):
    client_id = auth.get('clientId')
    team_id = auth.get('teamId')
    key_id = auth.get('keyId')

    # privateKey = auth.get('privateKey')
    privateKey = 'MHcCAQEEIJgiDLBqbaAb8pqgK74wEY/u0uiswAZkECJFkLUayk+9oAoGCCqGSM49AwEHoUQDQgAEfsYLIIQVzyQWizAguQWR9l7ZkXijRAzgJRXGuq/Q/th1FqlsFyE7vr4xDCw53+JoJebvKBy8QbZgSWON8TohdA=='
    key = '-----BEGIN EC PRIVATE KEY-----\n' + privateKey + '\n-----END EC PRIVATE KEY-----'
    
    # TODO get private key from s3 

    audience = 'https://appleid.apple.com'
    alg = 'ES256'

    # Define issue timestamp.
    issued_at_timestamp = int(dt.datetime.utcnow().timestamp())

    # Define expiration timestamp. May not exceed 1 days from issue timestamp.
    expiration_timestamp = issued_at_timestamp + 86400*1

    # Define JWT headers.
    headers = dict()
    headers['alg'] = alg
    headers['kid'] = key_id

    # Define JWT payload.
    payload = dict()
    payload['sub'] = client_id
    payload['aud'] = audience
    payload['iat'] = issued_at_timestamp
    payload['exp'] = expiration_timestamp
    payload['iss'] = team_id

    print("issued_at_timestamp:::" + str(issued_at_timestamp))
    print("expiration_timestamp:::" + str(expiration_timestamp)) 

    dprint("\nPayload %s" % str(payload))
    dprint("\nHeaders %s" % str(headers))
    dprint("\nKey %s" % str(key))
    
    client_secret = jwt.encode(
        payload=payload,  
        headers=headers,
        key=key,
        algorithm=alg
    )

    # use client secret to get auth token
    url = config.APPLE_AUTH_URL

    headers = {
        "Host": "appleid.apple.com", 
        "Content-Type": "application/x-www-form-urlencoded"
    }
    params = {
        "grant_type" : "client_credentials",
        "client_id" : client_id,
        "client_secret" : client_secret,
        "scope" : "searchadsorg"
    }
    dprint("\nURL is %s" % url)
    dprint("\nHeaders are %s" % headers)
    dprint("\nParams are %s" % params)

    response = requests.post(url, params=params, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT)

    print("response is " + str(response))
    print("response text " + str(response.text))

    return json.loads(response.text).get("access_token", None)
