from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
client = boto3.client('dynamodb')

# CREATE DB TABLES
print("existing_tables:::" + str(client.list_tables()))
existing_tables = client.list_tables()['TableNames']

if 'branch_commerce_events' not in existing_tables:
    table = dynamodb.create_table(
        TableName='branch_commerce_events',
        KeySchema=[
            {
            'AttributeName': 'branch_commerce_event_key',
            'KeyType': 'HASH'  #Partition key defined by concatenating campaign_id, ad_set_id and keyword
            },
            {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'branch_commerce_event_key',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'keyword',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'campaign_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'ad_set_id',
                'AttributeType': 'S'
            },
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'campaign_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'campaign_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'keyword-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'ad_set_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'ad_set_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)


if 'branch_opens' not in existing_tables:
    table = dynamodb.create_table(
        TableName='branch_opens',
        KeySchema=[
        {
            'AttributeName': 'branch_open_key',
            'KeyType': 'HASH'  #Partition key defined by concatenating campaign_id, ad_set_id and keyword
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
        },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'branch_open_key',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'keyword',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'campaign_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'ad_set_id',
                'AttributeType': 'S'
            },
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'campaign_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'campaign_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'keyword-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'ad_set_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'ad_set_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)


if 'branch_installs' not in existing_tables:
    table = dynamodb.create_table(
    TableName='branch_installs',
    KeySchema=[
        {
            'AttributeName': 'branch_install_key',
            'KeyType': 'HASH' #Partition key defined by concatenating campaign_id, ad_set_id and keyword
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
        },
    ],
        AttributeDefinitions=[
            {
                'AttributeName': 'branch_install_key',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'keyword',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'campaign_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'ad_set_id',
                'AttributeType': 'S'
            },
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'campaign_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'campaign_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'keyword-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'ad_set_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'ad_set_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)


if 'branch_reinstalls' not in existing_tables:
    table = dynamodb.create_table(
        TableName='branch_reinstalls',
        KeySchema=[
        {
            'AttributeName': 'branch_reinstall_key',
            'KeyType': 'HASH'  #Partition key defined by concatenating campaign_id, ad_set_id and keyword
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
        },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'branch_reinstall_key',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'keyword',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'campaign_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'ad_set_id',
                'AttributeType': 'S'
            },
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'campaign_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'campaign_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'keyword-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'keyword',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'ad_set_id-timestamp-index',
                'KeySchema': [
                    {
                        'AttributeName': 'ad_set_id',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'timestamp',
                        'KeyType': 'RANGE'  # Sort key
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)


if 'cpi_history' not in existing_tables:
    table = dynamodb.create_table(
        TableName='cpi_history',
        KeySchema=[
        {
            'AttributeName': 'org_id',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
        },
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'timestamp',
            'AttributeType': 'S'
        }
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Table status:", table.table_name, table.table_status)

if 'bids' not in existing_tables:
    table = dynamodb.create_table(
        TableName='bids',
        KeySchema=[
        {
            'AttributeName': 'org_id',
            'KeyType': 'HASH'  #Partition key
        }
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)

if 'adgroup_bids' not in existing_tables:
    table = dynamodb.create_table(
        TableName='adgroup_bids',
        KeySchema=[
        {
            'AttributeName': 'org_id',
            'KeyType': 'HASH'  #Partition key
        }
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)

if 'negative_keywords' not in existing_tables:
    table = dynamodb.create_table(
        TableName='negative_keywords',
        KeySchema=[
            {
                'AttributeName': 'org_id',
                'KeyType': 'HASH'  #Partition key
            }
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)

if 'positive_keywords' not in existing_tables:
    table = dynamodb.create_table(
        TableName='positive_keywords',
        KeySchema=[
        {
            'AttributeName': 'org_id',
            'KeyType': 'HASH'  #Partition key
        }
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)