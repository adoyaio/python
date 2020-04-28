import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key

def getBranchCommerceEvents(dynamoResource, ad_set_id, keyword, timestamp):
    table = dynamoResource.Table('branch_commerce_events')

    response = table.query(
        KeyConditionExpression=Key('keyword').eq(keyword) & Key('timestamp').eq(timestamp),
        IndexName='keyword-timestamp-index'
    )
    # print('getBranchCommerceEvents' + response['Items']);
    return response


def getAppleKeywordData(dynamoResource, ad_group_id, start_date, end_date):
    table = dynamoResource.Table('apple_keyword')
    response = table.query(
        KeyConditionExpression=Key('adgroup_id').eq(str(ad_group_id)) & Key('date').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d')),
        IndexName='adgroup_id-timestamp-index'
    )

    return response