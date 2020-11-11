import decimal
import json
import boto3
from botocore.exceptions import ClientError

# this job is to populate branch_commerce_events table with orgId
dynamodbLocal = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")

# PROD
dynamodbProd = boto3.resource('dynamodb', region_name='us-east-1')

tableName = 'branch_commerce_events'

local = dynamodbLocal.Table(tableName)
prod = dynamodbProd.Table(tableName)

# fix local
# items = []
# scan_kwargs = {}
# done = False
# start_key = None
# while not done:
#     if start_key:
#         scan_kwargs['ExclusiveStartKey'] = start_key
#     response = local.scan(**scan_kwargs)
#     items.extend(response.get('Items', []))
#     start_key = response.get('LastEvaluatedKey', None)
#     done = start_key is None

# # load each item extended with org_id
# for item in items:
#     new_item = {**item, 'org_id': '1056410'}
#     try:
#         response = local.put_item(
#             Item=new_item
#         )
#     except ClientError as e:
#         print(tableName + " failed due to" + e.response['Error']['Message'])

# print(tableName + " rows added:::" + str(len(items)))

# fix prod
items = []
scan_kwargs = {}
done = False
start_key = None
while not done:
    if start_key:
        scan_kwargs['ExclusiveStartKey'] = start_key
    response = local.scan(**scan_kwargs)
    items.extend(response.get('Items', []))
    start_key = response.get('LastEvaluatedKey', None)
    done = start_key is None

# load each item extended with org_id
for item in items:
    try:
        response = prod.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))