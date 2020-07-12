from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('clients')

 # LOAD SAMPLE DATA json
with open("../clients.json") as json_file:
    clients = json.load(json_file, parse_float=decimal.Decimal)
    for client in clients:
        orgId = client['orgId']
        currency = client['currency']
        print("Adding client line:", str(client))
        table.put_item(
            Item = {
                'orgId': orgId,
                'orgDetails': client
            }
        )