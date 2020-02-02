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
else:
    table = dynamodb.Table('cpi_history')
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

    ###############   load covetly data    #################
    with open("../history_1105630.csv", "r") as handle:
        cpi_lines = handle.readlines()[-365:]

        for line in cpi_lines:
            tokens = line.rstrip().split(",")
            if (tokens[0]) != "Date":
                timestamp = tokens[0]
                spend = tokens[1]
                installs = int(tokens[2])
                cpi = tokens[3]
                org_id = "1105630"
                print("Adding cpi line:", timestamp, spend[1:], installs, cpi[1:], org_id)
                table.put_item(
                    Item={
                        'timestamp': timestamp,
                        'spend': spend[1:],
                        'installs': installs,
                        'cpi': cpi[1:],
                    'org_id': org_id
                    }
                )

    ###############   load laundrie data   #################
    with open("../history_971540.csv", "r") as handle:
        cpi_lines = handle.readlines()[-365:]

        for line in cpi_lines:
            tokens = line.rstrip().split(",")
            if (tokens[0]) != "Date":
                timestamp = tokens[0]
                spend = tokens[1]
                installs = int(tokens[2])
                cpi = tokens[3]
                org_id = "971540"
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

    ###############   load her data     #################
    with open("../history_1056410.csv", "r") as handle:
        cpi_lines = handle.readlines()[-365:]

        for line in cpi_lines:
            tokens = line.rstrip().split(",")
            if (tokens[0]) != "Date":
                timestamp = tokens[0]
                spend = tokens[1]
                installs = int(tokens[2])
                cpi = tokens[3]
                org_id = "1056410"
                print("Adding cpi line:", timestamp, spend[1:], installs, cpi[1:], org_id)
                table.put_item(
                    Item={
                        'timestamp': timestamp,
                        'spend': spend[1:],
                        'installs': installs,
                        'cpi': cpi[1:],
                        'org_id': org_id
                    }
                )


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