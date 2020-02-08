from __future__ import print_function # Python 2/3 compatibility

import decimal
import json
import boto3

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

table = dynamodb.Table('cpi_history')

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
            print("Adding cpi line:", timestamp, spend.strip(), installs, cpi.strip(), org_id)
            table.put_item(
                Item={
                    'timestamp': timestamp,
                    'spend': spend.strip(),
                    'installs': installs,
                    'cpi': cpi.strip(),
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
