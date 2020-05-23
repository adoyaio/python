import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key

def getBranchCommerceEvents(dynamoResource, ad_set_id, keyword, timestamp):
    table = dynamoResource.Table('branch_commerce_events')

    # TODO add ad set id to key
    response = table.query(
        KeyConditionExpression=Key('keyword').eq(keyword) & Key('timestamp').eq(timestamp),
        IndexName='keyword-timestamp-index'
    )
    # print('getBranchCommerceEvents' + response['Items']);
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
                #print(json.dumps(i, cls=DecimalEncoder))

        print("getBranchPurchasesForTimeperiod:::" + str(total_branch_events))
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
            total_branch_revenue += int(i['revenue'])
            # print(json.dumps(i, cls=DecimalEncoder))

    print("getBranchRevenueForTimeperiod:::" + str(total_branch_revenue))
    return total_branch_revenue

def getAppleKeywordData(dynamoResource, ad_group_id, start_date, end_date):
    table = dynamoResource.Table('apple_keyword')
    response = table.query(
        KeyConditionExpression=Key('adgroup_id').eq(str(ad_group_id)) & Key('date').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
        IndexName='adgroup_id-timestamp-index'
    )

    return response