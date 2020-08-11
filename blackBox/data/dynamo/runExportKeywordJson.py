import datetime
import decimal
import json
import logging
import pprint
import boto3

from boto3.dynamodb.conditions import Key, Attr

sendG = False  # Set to True to enable sending data to Apple, else a test run.
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


# ------------------------------------------------------------------------------
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


# ------------------------------------------------------------------------------
def process():
    data = []
    table = dynamodb.Table('apple_keyword')
    adgroup_id = "197913349"
    # response = table.scan(
    #     Limit=100
    # )

    response = table.scan()
    # response = table.query(
    #     KeyConditionExpression=Key('adgroup_id').eq(adgroup_id),
    #     ScanIndexForward=False,
    #     Limit=100,
    #     IndexName='adgroup_id-timestamp-index'
    # )

    # response = table.query(
    #     KeyConditionExpression=Key('campaign_id').eq(adgroup_id),
    #     ScanIndexForward=False,
    #     Limit=100
    #    IndexName='adgroup_id-timestamp-index'
    # )

    for keyword in (response["Items"]):
        data.append(keyword)

    with open('keywords.' + str(today) + '.json', 'w') as outfile:
        json.dump(data, outfile, cls=DecimalEncoder, indent=4)

# ------------------------------------------------------------------------------
def terminate():
    pass


# ------------------------------------------------------------------------------
if __name__ == "__main__":
    initialize('lcl', 'http://localhost:8000')
    process()
    terminate()

    # initialize('prod', '')
    # process()
    # terminate()