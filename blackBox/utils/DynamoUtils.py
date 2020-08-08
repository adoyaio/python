import datetime
import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key
from decimal import *
import json
dashG = "-"


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

def getBranchCommerceEvents(dynamoResource, campaign_id, ad_set_id, keyword, timestamp):
    table = dynamoResource.Table('branch_commerce_events')
   
    # normalize search term to how its being stored in db
    event_key = str(campaign_id) + dashG + str(ad_set_id) + dashG + keyword.replace(" ", dashG)
    response = table.query(
        KeyConditionExpression=Key('branch_commerce_event_key').eq(event_key) & Key('timestamp').eq(timestamp),
    )
    return response

def getBranchPurchasesForTimeperiod(dynamoResource, campaign_id, start_date, end_date):
        table = dynamoResource.Table('branch_commerce_events')
        response = table.query(
            KeyConditionExpression=Key('campaign_id').eq(str(campaign_id)) & Key('timestamp').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
            IndexName='campaign_id-timestamp-index'
        )
        total_branch_events = 0
        if len(response['Items']) >= 0:
            for i in response[u'Items']:
                total_branch_events += int(i['count'])

        return total_branch_events


def getBranchRevenueForTimeperiod(dynamoResource, campaign_id, start_date, end_date):
    table = dynamoResource.Table('branch_commerce_events')
    response = table.query(
        KeyConditionExpression=Key('campaign_id').eq(str(campaign_id)) & Key('timestamp').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
        IndexName='campaign_id-timestamp-index'
    )
    total_branch_revenue = 0
    if len(response['Items']) >= 0:
        for i in response[u'Items']:
            total_branch_revenue += float(i['revenue'])

    return total_branch_revenue

def getAppleKeywordData(dynamoResource, ad_group_id, start_date, end_date):
    table = dynamoResource.Table('apple_keyword')
    response = table.query(
        KeyConditionExpression=Key('adgroup_id').eq(str(ad_group_id)) & Key('date').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
        IndexName='adgroup_id-timestamp-index'
    )
    return response

# cast to int here because client table was migrated from client.json 
# TODO in dynamo numbers are serialized so there is no advantage to using int consider using string for consistency
def getClient(dynamoResource, client_id):
    table = dynamoResource.Table('clients')
    response = table.query(
        KeyConditionExpression=Key('orgId').eq(int(client_id))
    )
    return response['Items']


def getClientHistory(dynamoResource, client_id):
    today = datetime.date.today()
    end_date_delta = datetime.timedelta(days=1)
    start_date_delta = datetime.timedelta(365)
    start_date = today - start_date_delta
    end_date = today - end_date_delta

    table = dynamoResource.Table('cpi_history')
    response = table.query(
        KeyConditionExpression=Key('org_id').eq(client_id & Key('timestamp').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        ))
    return response['Items']


def getClientHistoryByTime(dynamoResource, client_id, start_date, end_date):
    table = dynamoResource.Table('cpi_history')
    response = table.query(
        KeyConditionExpression=Key('org_id').eq(client_id) & Key('timestamp').between(end_date, start_date),
    )
    return response['Items']


def getClientHistoryNumRecs(dynamoResource, client_id, total_recs):
    table = dynamoResource.Table('cpi_history')
    response = table.query(
        KeyConditionExpression=Key('org_id').eq(client_id),
        ScanIndexForward=False,
        Limit=int(total_recs)
    )
    return response['Items']


def getClientBranchHistoryByTime(dynamoResource, client_id, start_date, end_date):
    table = dynamoResource.Table('cpi_branch_history')
    response = table.query(
        KeyConditionExpression=Key('org_id').eq(client_id) & Key('timestamp').between(end_date, start_date),
    )
    return response['Items']


def getClientBranchHistoryNumRecs(dynamoResource, client_id, total_recs):
    table = dynamoResource.Table('cpi_branch_history')
    response = table.query(
        KeyConditionExpression=Key('org_id').eq(client_id),
        ScanIndexForward=False,
        Limit=int(total_recs)
    )
    return response['Items']