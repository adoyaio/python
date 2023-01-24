from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
client = boto3.client('dynamodb', region_name='us-east-1')

# CREATE DB TABLES
print("existing_tables:::" + str(client.list_tables()))
existing_tables = client.list_tables()['TableNames']


response = client.update_table(
    TableName='campaign_branch_history',
    AttributeDefinitions=[
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'campaign_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'timestamp',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'spend',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'cpi',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'cpp',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'revenue',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'installs',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'revenueOverCost',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'purchases',
            'AttributeType': 'N'
        },
    ],
    GlobalSecondaryIndexUpdates=[
        {
          'Create': {
            'IndexName': 'org_id-timestamp-index',
            'KeySchema': [
              {
                'AttributeName': 'org_id',
                'KeyType': 'HASH'
              },
              {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'
              }
            ],
            'Projection': {
              'ProjectionType': 'ALL'
            }
          }
        }
    ]
)