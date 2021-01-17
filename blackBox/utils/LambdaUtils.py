import boto3
import botocore.config
import logging
import json
import decimal
from Client import Client
from utils.DecimalEncoder import DecimalEncoder

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


def getClientForLocalRun(orgId):
    clientEvent = {}
    clientEvent['rootEvent'] = {
        "env": "lcl",
        "dynamoEndpoint": "http://localhost:8000",
        "lambdaEndpoint": "http://host.docker.internal:3001",
        "emailToInternal": ["james@adoya.io"]
    }
    with open("./data/dynamo/clients.json") as json_file:
        clients = json.load(json_file, parse_float=decimal.Decimal)
        clientJSON = next(item for item in clients if item["orgId"] == orgId)
        client = Client(
            clientJSON['orgId'],
            clientJSON['clientName'],
            clientJSON['emailAddresses'],
            clientJSON['keyFilename'],
            clientJSON['pemFilename'],
            clientJSON['bidParameters'],
            clientJSON['adgroupBidParameters'],
            clientJSON['branchBidParameters'],
            clientJSON['campaignIds'],
            clientJSON['keywordAdderIds'],
            clientJSON['keywordAdderParameters'],
            clientJSON['branchIntegrationParameters'],
            clientJSON['currency'],
            clientJSON['appName'],
            clientJSON['appID'],
            clientJSON['campaignName']
        )     
    clientEvent['orgDetails'] = json.dumps(client.__dict__,cls=DecimalEncoder)
    return clientEvent