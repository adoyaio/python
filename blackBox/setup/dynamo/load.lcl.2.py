from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3
from botocore.exceptions import ClientError

dynamodbLocal = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
dynamodbProd = boto3.resource('dynamodb', region_name='us-east-1')


def load_items_to_local(items, local, tableName):
    for item in items:
        try:
            response = local.put_item(
                Item=item
            )
        except ClientError as e:
            print(tableName + " failed due to" + e.response['Error']['Message'])
    print(tableName + " rows added:::" + str(len(items)))


if __name__ == '__main__':
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
    # items = prod.scan()["Items"]

    done = False
    start_key = None
    scan_kwargs = {}

    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = prod.scan(**scan_kwargs)
        load_items_to_local(response.get('Items', []), local, tableName)
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None


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