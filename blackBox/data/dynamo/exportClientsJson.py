import datetime
import decimal
import json
import logging
import pprint
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
date = datetime.date
today = datetime.date.today()

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def initialize(env, dynamoEndpoint):
    global sendG
    global dynamodb

    if env == "lcl":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
        logger.setLevel(logging.INFO)
    elif env == "prod":
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)


def process():
    data = []
    for client in (dynamodb.Table('clients').scan()["Items"]):
        # data.append(client["orgDetails"])
        data.append(client)
    with open('clients.' + str(today) + '.json', 'w') as outfile:
        json.dump(data, outfile, cls=DecimalEncoder, indent=4, sort_keys=True)


if __name__ == "__main__":
    # initialize('lcl', 'http://localhost:8000')
    # process()

    initialize('prod', '')
    process()