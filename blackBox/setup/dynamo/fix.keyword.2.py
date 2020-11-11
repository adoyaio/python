import decimal
import json
import boto3
from botocore.exceptions import ClientError

# this job is to populate apple_keyword table with orgId and campaignId

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")

# PROD
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

tableName = 'apple_keyword'
table = dynamodb.Table(tableName)


items = table.scan()["Items"]

for item in items:
    # load each item to 
    adgroupId = item["adgroup_id"]

    try:
        response = table.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))
