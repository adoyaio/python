from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
client = boto3.client('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")

# CREATE DB TABLES
print("existing_tables:::" + str(client.list_tables()))
existing_tables = client.list_tables()['TableNames']

if 'branch_commerce_events' not in existing_tables:
    table = dynamodb.create_table(
        TableName='branch_commerce_events',
        KeySchema=[
            {
            'AttributeName': 'branch_commerce_event_key',
            'KeyType': 'HASH'  #Partition key TODO should use org_id and # seperator defined by concatenating campaign_id, ad_set_id and keyword
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
            {
                'AttributeName': 'org_id',
                'AttributeType': 'S'
            }
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
                        'KeyType': 'HASH'  # Partition key TODO should use org_id#keyword
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
            },
            {
                'IndexName': 'org_id-timestamp-index', # TODO could be removed if org_id is added to branch_commerce_event_key
                'KeySchema': [
                    {
                        'AttributeName': 'org_id',
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
                'KeyType': 'HASH'  # Partition key defined by concatenating campaign_id, ad_set_id and keyword
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'  # Sort key
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

if 'apple_adgroup' not in existing_tables:
    table = dynamodb.create_table(
        TableName='apple_adgroup',
        KeySchema=[
        {
            'AttributeName': 'adgroup_id',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'date',
            'KeyType': 'RANGE'  #Sort key
        },
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'date',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'app_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'campaign_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'adgroup_id',
            'AttributeType': 'S'
        }
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
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'app_id-timestamp-index',
            'KeySchema': [
                {
                    'AttributeName': 'app_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        }
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Table status:", table.table_name, table.table_status)


if 'apple_keyword' not in existing_tables:
    table = dynamodb.create_table(
        TableName='apple_keyword',
        KeySchema=[
        {
            'AttributeName': 'keyword_id',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'date',
            'KeyType': 'RANGE'  #Sort key
        },
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'keyword_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'date',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'app_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'campaign_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'adgroup_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'keyword',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        }
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
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'app_id-timestamp-index',
            'KeySchema': [
                {
                    'AttributeName': 'app_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'adgroup_id-timestamp-index',
            'KeySchema': [
                {
                    'AttributeName': 'adgroup_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'date',
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
                    'KeyType': 'HASH'  # Partition key, TODO delete? use kw id
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-timestamp-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        }
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Table status:", table.table_name, table.table_status)

# dynamo numbers are serialized to strings so no advantage to using number here
# TODO rework to use string key instead of number 
if 'clients_2' not in existing_tables:
    table = dynamodb.create_table(
        TableName='clients_2',
        KeySchema=[
            {
                'AttributeName': 'orgId',
                'KeyType': 'HASH'  #Partition key
            }
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'orgId',
            'AttributeType': 'S' 
        }
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    print("Table status:", table.table_name, table.table_status)

if 'cpi_branch_history' not in existing_tables:
    table = dynamodb.create_table(
        TableName='cpi_branch_history',
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
        },
                {
            'AttributeName': 'cpp',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'revenue',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'spend',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'cpi',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'installs',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'revenueOverCost',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'purchases',
            'AttributeType': 'N'
        },
        ],
        GlobalSecondaryIndexes=[
        {
            'IndexName': 'org_id-cpp-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'cpp',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-revenue-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'revenue',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-purchases-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'purchases',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-spend-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'spend',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-cpi-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'cpi',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-installs-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'installs',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-revenueOverCost-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'revenueOverCost',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Table status:", table.table_name, table.table_status)

if 'apple_branch_keyword' not in existing_tables:
    table = dynamodb.create_table(
        TableName='apple_branch_keyword',
        KeySchema=[
        {
            'AttributeName': 'keyword_id',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'date',
            'KeyType': 'RANGE'  #Sort key
        },
        ],
        AttributeDefinitions=[
        {
            'AttributeName': 'keyword_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'date',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'app_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'campaign_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'adgroup_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'keyword',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'org_id',
            'AttributeType': 'S'
        }
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
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'app_id-timestamp-index',
            'KeySchema': [
                {
                    'AttributeName': 'app_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'adgroup_id-timestamp-index',
            'KeySchema': [
                {
                    'AttributeName': 'adgroup_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'date',
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
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'org_id-timestamp-index',
            'KeySchema': [
                {
                    'AttributeName': 'org_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'date',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        }
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Table status:", table.table_name, table.table_status)

if 'campaign_branch_history' not in existing_tables:
    table = dynamodb.create_table(
        TableName='campaign_branch_history',
        KeySchema=[
        {
            'AttributeName': 'campaign_id',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'timestamp',
            'KeyType': 'RANGE'  #Sort key
        },
        ],
        AttributeDefinitions=[
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
        GlobalSecondaryIndexes=[
        {
            'IndexName': 'campaign_id-cpp-index',
            'KeySchema': [
                {
                    'AttributeName': 'campaign_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'cpp',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'campaign_id-revenue-index',
            'KeySchema': [
                {
                    'AttributeName': 'campaign_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'revenue',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'campaign_id-purchases-index',
            'KeySchema': [
                {
                    'AttributeName': 'campaign_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'purchases',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'campaign_id-spend-index',
            'KeySchema': [
                {
                    'AttributeName': 'campaign_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'spend',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'campaign_id-cpi-index',
            'KeySchema': [
                {
                    'AttributeName': 'campaign_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'cpi',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'campaign_id-installs-index',
            'KeySchema': [
                {
                    'AttributeName': 'campaign_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'installs',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        {
            'IndexName': 'campaign_id-revenueOverCost-index',
            'KeySchema': [
                {
                    'AttributeName': 'campaign_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'revenueOverCost',
                    'KeyType': 'RANGE'  # Sort key
                },
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            }
        },
        ],
        BillingMode="PAY_PER_REQUEST"
    )
    print("Table status:", table.table_name, table.table_status)
