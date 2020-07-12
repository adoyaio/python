from __future__ import print_function # Python 2/3 compatibility
import decimal
import json
import boto3
import botocore.exceptions
from datetime import datetime
import sys
sys.path.insert(1, '/Users/jafarris/Documents/adoya/python/blackBox/utils/')
import DynamoUtils

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")

# mapping of org_ids to campaign ids
mapping = {}
mapping['1056410'] = ['197914192', '197913328']
mapping['1105630'] = ['158675458', '187214904']

# source tables
cpiHistoryTable = dynamodb.Table('cpi_history')

# destination table
cpiBranchHistoryTable = dynamodb.Table('cpi_branch_history')

# load cpi_history and branch_commerce_events into cpi_branch_history
cpiItems = cpiHistoryTable.scan()["Items"]
for cpiItem in cpiItems:
    try:
        # pull fields from cpiItem
        org_id = cpiItem["org_id"]

        if org_id != '971540':
            timestamp = cpiItem["timestamp"]
            cpi = cpiItem["cpi"]
            installs = cpiItem["installs"]
            spend = cpiItem["spend"]
            branch_revenue = 0
            branch_purchases = 0

            # iterate campaign ids and query branch_commerce_events for timestamp 
            for campaignId in mapping[org_id]:
                branch_revenue = branch_revenue + DynamoUtils.getBranchRevenueForTimeperiod(dynamodb, campaignId, datetime.strptime(timestamp, '%Y-%m-%d'), datetime.strptime(timestamp, '%Y-%m-%d'))
                branch_purchases = branch_purchases + DynamoUtils.getBranchPurchasesForTimeperiod(dynamodb, campaignId, datetime.strptime(timestamp, '%Y-%m-%d'), datetime.strptime(timestamp, '%Y-%m-%d'))
        
            item = {
            'org_id':org_id,
            'timestamp':timestamp,
            'cpi':cpi,
            'installs':installs,
            'spend':spend,
            'branch_revenue':branch_revenue,
            'branch_purchases':branch_purchases
            }
            response = cpiBranchHistoryTable.put_item(
                Item=item
            )
    
    except botocore.exceptions.ClientError as e:
        print("cpi_history failed due to" + e.response['Error']['Message'])

print("cpi_branch_history rows added:::" + str(len(cpiItems)))
 