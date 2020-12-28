import boto3
import logging

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
    if env == "lcl":
        return boto3.client('lambda', region_name='us-east-1', endpoint_url=endpoint)
    elif env == "stage":
        return boto3.client('lambda', region_name='us-east-1', endpoint_url=endpoint)
    elif env == "prod":
        return boto3.client('lambda', region_name='us-east-1')
    else:
        return boto3.client('lambda', region_name='us-east-1')
    
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
