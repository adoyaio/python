import decimal
import json
import boto3
from botocore.exceptions import ClientError

# this job is to populate apple_keyword table with orgId and campaignId

# dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")

# PROD
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

tableName = 'apple_keyword'
table = dynamodb.Table(tableName)

# mapping of adgroupId to campaignId & orgId
campaignMapping = {}
campaignMapping['197914177'] = '197914192'
campaignMapping['197913349'] = '197913328'
campaignMapping['158698070'] = '158675458'
campaignMapping['187192993'] = '187214904'

orgMapping = {}
orgMapping['197914177'] = '1056410'
orgMapping['197913349'] = '1056410'
orgMapping['158698070'] = '1105630'
orgMapping['187192993'] = '1105630'

items = table.scan()["Items"]

for item in items:
    # build the updated item
    adgroupId = item["adgroup_id"]
    item["campaign_id"] = campaignMapping[adgroupId]
    item["org_id"] = orgMapping[adgroupId]
    try:
        response = table.put_item(
            Item=item
        )
    except ClientError as e:
        print(tableName + " failed due to" + e.response['Error']['Message'])

print(tableName + " rows added:::" + str(len(items)))
