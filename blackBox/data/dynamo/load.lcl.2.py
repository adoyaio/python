from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3
from botocore.exceptions import ClientError

dynamodbLocal = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
dynamodbProd = boto3.resource('dynamodb', region_name='us-east-1')

# client table
clientsTableLocal = dynamodbLocal.Table('clients')
clientsTableProd = dynamodbProd.Table('clients')
items = clientsTableProd.scan()["Items"]
for item in items:
    try:
        response = clientsTableLocal.put_item(
            Item=item
        )
    except ClientError as e:
        print("clients failed due to" + e.response['Error']['Message'])
    else:
        print("clients PutItem succeeded:")

# cpi_history table
clientsTableLocal = dynamodbLocal.Table('cpi_history')
clientsTableProd = dynamodbProd.Table('cpi_history')
items = clientsTableProd.scan()["Items"]
for item in items:
    try:
        response = clientsTableLocal.put_item(
            Item=item
        )
    except ClientError as e:
        print("cpi_history failed due to" + e.response['Error']['Message'])
    else:
        print("cpi_history PutItem succeeded:")




 # LOAD SAMPLE DATA json
# with open("../clients.json") as json_file:
#     clients = json.load(json_file, parse_float=decimal.Decimal)
#     for client in clients:
#         orgId = client['orgId']
#
#         print("Adding client line:", str(client))
#
#         table.put_item(
#             Item = {
#                 'orgId': orgId,
#                 'orgDetails': client
#             }
#         )