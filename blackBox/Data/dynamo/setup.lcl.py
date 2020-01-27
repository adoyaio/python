from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")

# CREATE DB TABLES
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

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
print("Table status:", table.table_name, table.table_status)

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

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
print("Table status:", table.table_name, table.table_status)

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

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
print("Table status:", table.table_name, table.table_status)

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

    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)
print("Table status:", table.table_name, table.table_status)

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
    BillingMode="PAY_PER_REQUEST",
    # ProvisionedThroughput={
    #     'ReadCapacityUnits': 10,
    #     'WriteCapacityUnits': 10
    # }
)
print("Table status:", table.table_name, table.table_status)

# LOAD SAMPLE DATA json
# with open("history_sample_data.lcl.json") as json_file:
#     cpi_lines = json.load(json_file, parse_float=decimal.Decimal)
#     for cpi_line in cpi_lines:
#         timestamp = cpi_line['date']
#         spend = cpi_line['spend']
#         installs = int(cpi_line['installs'])
#         cpi = cpi_line['cpi']
#         org_id = cpi_line['org_id']
#
#         print("Adding cpi line:", org_id, timestamp)
#
#         table.put_item(
#            Item={
#                'timestamp': timestamp,
#                'spend': spend,
#                'installs': installs,
#                'cpi': cpi,
#                'org_id': org_id
#             }
#         )

with open("../history_1105630.csv", "r") as handle:
    cpi_lines = handle.readlines()[-365:]

    for line in cpi_lines:
        tokens = line.rstrip().split(",")
        if (tokens[2]) != "Installs":
            timestamp = tokens[0]
            spend = tokens[1]
            installs = int(tokens[2])
            cpi = tokens[3]
            org_id = "1105630"
            print("Adding cpi line:", timestamp, spend, installs, cpi, org_id)
            table.put_item(
                Item={
                    'timestamp': timestamp,
                    'spend': spend,
                    'installs': installs,
                    'cpi': cpi,
                    'org_id': org_id
                }
            )


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