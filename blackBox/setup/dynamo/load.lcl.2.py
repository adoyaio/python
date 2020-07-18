from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3
from botocore.exceptions import ClientError

dynamodbLocal = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
dynamodbProd = boto3.resource('dynamodb', region_name='us-east-1')

# client table
tableName = 'clients'
local = dynamodbLocal.Table(tableName)
prod = dynamodbProd.Table(tableName)
items = prod.scan()["Items"]

for item in items:
    try:
        response = local.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))

# cpi_history table
tableName = 'cpi_history'
local = dynamodbLocal.Table(tableName)
prod = dynamodbProd.Table(tableName)
items = prod.scan()["Items"]

for item in items:
    try:
        response = local.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))

# apple_keyword table
tableName = 'apple_keyword'
local = dynamodbLocal.Table(tableName)
prod = dynamodbProd.Table(tableName)
items = prod.scan()["Items"]

for item in items:
    try:
        response = local.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))


# branch_commerce_events
tableName = 'branch_commerce_events'
local = dynamodbLocal.Table(tableName)
prod = dynamodbProd.Table(tableName)
items = prod.scan()["Items"]

for item in items:
    try:
        response = local.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))

# cpi_branch_history table
tableName = 'cpi_branch_history'
local = dynamodbLocal.Table(tableName)
prod = dynamodbProd.Table(tableName)
items = prod.scan()["Items"]

for item in items:
    try:
        response = local.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))