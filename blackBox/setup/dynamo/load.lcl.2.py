import datetime
import decimal
import json
import boto3
import sys
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodbLocal = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url="http://localhost:8000")
dynamodbProd = boto3.resource('dynamodb', region_name='us-east-1')


def load_items_to_local(items, local, tableName):
    for item in items:
        try:
            response = local.put_item(
                Item=item
            )
        except ClientError as e:
            print(tableName + " failed due to" + e.response['Error']['Message'])
    print(tableName + " rows added:::" + str(len(items)))


if __name__ == '__main__':
    # get input client id
    orgId = sys.argv[1]
    print('Client Id:' + str(orgId)) 

    # client table
    tableName = 'clients'
    local = dynamodbLocal.Table(tableName)
    prod = dynamodbProd.Table(tableName)
    prodResponse = prod.query(
        KeyConditionExpression=Key('orgId').eq(int(orgId))
    )
    for item in prodResponse.get("Items"):
        try:
            localResponse = local.put_item(
                Item=item
            )
        except ClientError as e:
            print(tableName + " failed due to" + e.localResponse['Error']['Message'])

    print(tableName + " rows added:::" + str(len(prodResponse.get("Items"))))

    # cpi_history table
    tableName = 'cpi_history'
    local = dynamodbLocal.Table(tableName)
    prod = dynamodbProd.Table(tableName)
    prodResponse = prod.query(
        KeyConditionExpression=Key('org_id').eq(str(orgId))
    )

    for item in prodResponse.get("Items"):
        try:
            localResponse = local.put_item(
                Item=item
            )
        except ClientError as e:
            print(tableName + " failed due to" + e.localResponse['Error']['Message'])

    print(tableName + " rows added:::" + str(len(prodResponse.get("Items"))))

    # apple_keyword table
    tableName = 'apple_keyword'
    local = dynamodbLocal.Table(tableName)
    prod = dynamodbProd.Table(tableName)
    today = datetime.date.today()
    end_date_delta = datetime.timedelta(days=1)
    start_date_delta = datetime.timedelta(14)
    start_date = today - start_date_delta
    end_date = today - end_date_delta
    count = 0
    done = False
    start_key = None
    query_kwargs = {}
    # query_kwargs['KeyConditionExpression'] = Key('org_id').eq(str(orgId))
    query_kwargs['KeyConditionExpression'] = Key('org_id').eq(str(orgId)) & Key('date').between(
                start_date.strftime('%Y-%m-%d'), 
                end_date.strftime('%Y-%m-%d')
            )
    query_kwargs['IndexName'] = 'org_id-timestamp-index'    

    while not done:
        if start_key:
            query_kwargs['ExclusiveStartKey'] = start_key

        prodResponse = prod.query(**query_kwargs)
        load_items_to_local(prodResponse.get('Items', []), local, tableName)
        start_key = prodResponse.get('LastEvaluatedKey', None)
        count += 1
        # done = start_key is None or count == 10
        done = start_key is None

    # campaign_branch_history table
    print("james test")
    tableName = 'campaign_branch_history'
    local = dynamodbLocal.Table(tableName)
    prod = dynamodbProd.Table(tableName)
    today = datetime.date.today()
    end_date_delta = datetime.timedelta(days=1)
    start_date_delta = datetime.timedelta(14)
    start_date = today - start_date_delta
    end_date = today - end_date_delta
    count = 0
    done = False
    start_key = None
    query_kwargs = {}

    # first hit clients to get list of 
    clientTable = dynamodbLocal.Table('clients')
    clientTableResponse = clientTable.query(
        KeyConditionExpression=Key('orgId').eq(int(orgId))
    )

    # print(str(clientTableResponse.get('Items')[0].get('orgDetails').get('appleCampaigns')))

    campaigns = clientTableResponse.get('Items')[0].get('orgDetails').get('appleCampaigns')
    campaignIds = []
    formatted = json.dumps(clientTableResponse.get('Items')[0].get('orgDetails').get('appleCampaigns'), cls=DecimalEncoder, indent=2)
    # print(formatted)

    for campaign in json.loads(formatted):
        campaignIds.append(campaign['campaignId'])
        print(campaign['campaignId'])
        done = False

        query_kwargs['KeyConditionExpression'] = Key('campaign_id').eq(str(campaign['campaignId']))
        # query_kwargs['KeyConditionExpression'] = Key('campaign_id').eq(str(campaign['campaignId'])) & Key('timestamp').between(
        #             start_date.strftime('%Y-%m-%d'), 
        #             end_date.strftime('%Y-%m-%d')
        #         )
        while not done:
            if start_key:
                query_kwargs['ExclusiveStartKey'] = start_key

            prodResponse = prod.query(**query_kwargs)
            load_items_to_local(prodResponse.get('Items', []), local, tableName)
            start_key = prodResponse.get('LastEvaluatedKey', None)
            count += 1
            done = start_key is None




    # branch_commerce_events
    tableName = 'branch_commerce_events'
    local = dynamodbLocal.Table(tableName)
    prod = dynamodbProd.Table(tableName)
    prodResponse = prod.scan()

    for item in prodResponse.get("Items"):
        try:
            localResponse = local.put_item(
                Item=item
            )
        except ClientError as e:
            print(tableName + " failed due to" + e.localResponse['Error']['Message'])

    print(tableName + " rows added:::" + str(len(prodResponse.get("Items"))))

    # cpi_branch_history table
    tableName = 'cpi_branch_history'
    local = dynamodbLocal.Table(tableName)
    prod = dynamodbProd.Table(tableName)
    prodResponse = prod.query(
        KeyConditionExpression=Key('org_id').eq(str(orgId))
    )

    for item in prodResponse.get("Items"):
        try:
            localResponse = local.put_item(
                Item=item
            )
        except ClientError as e:
            print(tableName + " failed due to" + e.localResponse['Error']['Message'])

    print(tableName + " rows added:::" + str(len(prodResponse.get("Items"))))