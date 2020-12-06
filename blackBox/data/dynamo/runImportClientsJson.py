from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3

# UNCOMMENT FOR PROD
# dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# UNCOMMENT FOR LOCAL
dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")

table = dynamodb.Table('clients')

 # LOAD SAMPLE DATA json
with open("./clients.json") as json_file:
    clients = json.load(json_file, parse_float=decimal.Decimal)
    for client in clients:
        orgId = client['orgId']
        print("Adding client line:", str(client))
        table.put_item(
            Item = {
                'orgId': orgId,
                'orgDetails': client
            }
        )