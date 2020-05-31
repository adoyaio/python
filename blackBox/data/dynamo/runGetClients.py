from __future__ import print_function  # Python 2/3 compatibility

import decimal
import json
import logging
import pprint

import boto3

from debug import debug, dprint
from utils import DynamoUtils

sendG = False  # Set to True to enable sending data to Apple, else a test run.
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# ------------------------------------------------------------------------------
@debug
def initialize(env, dynamoEndpoint):
    global sendG
    global clientsG
    global dynamodb

    if env == "lcl":
        sendG = False
        # dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)
    elif env == "prod":
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)
    else:
        sendG = False
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        logger.setLevel(logging.INFO)

    clientsG = DynamoUtils.getClients(dynamodb)
    logger.info("In runPrintHistory:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


# ------------------------------------------------------------------------------
@debug
def process():

    data = []
    for client in (dynamodb.Table('clients').scan()["Items"]):
        data.append(client["orgDetails"])

    dprint("%s" % pprint.pformat(data))

    # json_object = json.loads(data, cls=DecimalEncoder)
    # json_formatted_str = json.dumps(data, cls=DecimalEncoder)

    # with open('clientsTest.json', 'w') as outfile:
    #     json.dump(json_formatted_str, outfile)

    with open('clientsTest.json', 'w') as outfile:
        json.dump(data, outfile, cls=DecimalEncoder, indent=4)


    # for client in clientsG:
    #     data.append(client.keyFilename)
    #     # print("Print CPI history for: " + str(client.clientName))
    #     # print(client.orgId)
    #     # history = client.getHistory(dynamodb);
    #     # dprint("%s" % pprint.pformat(history))
    #
    # with open('clientsTest.json', 'w') as outfile:
    #     json.dump(data, outfile)

# ------------------------------------------------------------------------------
@debug
def terminate():
    pass


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    initialize('lcl', 'http://localhost:8000')
    process()
    terminate()

    # initialize('prod', 'http://localhost:8000')
    # process()
    # terminate()