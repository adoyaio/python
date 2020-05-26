import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key

from Client import Client

dashG = "-"

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

def getClients(dynamoResource):
    CLIENTS = []
    for client in (dynamoResource.Table('clients').scan()["Items"]):
        CLIENTS.append(Client(client["orgDetails"]["orgId"],
                              client["orgDetails"]["clientName"],
                              client["orgDetails"]["emailAddresses"],
                              client["orgDetails"]["keyFilename"],
                              client["orgDetails"]["pemFilename"],
                              client["orgDetails"]["bidParameters"],
                              client["orgDetails"]["adgroupBidParameters"],
                              client["orgDetails"]["branchBidParameters"],
                              client["orgDetails"]["campaignIds"],
                              client["orgDetails"]["keywordAdderIds"],
                              client["orgDetails"]["keywordAdderParameters"],
                              client["orgDetails"]["branchIntegrationParameters"],
                              client["orgDetails"]["currency"],
                              client["orgDetails"]["appName"],
                              client["orgDetails"]["appID"],
                              client["orgDetails"]["campaignName"]
                              ))

    for client in CLIENTS:
        for bidParam in client.bidParameters:
            client.bidParameters[bidParam] = float(client.bidParameters.get(bidParam))

        for bidParam in client.adgroupBidParameters:
            client.adgroupBidParameters[bidParam] = float(client.adgroupBidParameters.get(bidParam))

        for bidParam in client.branchBidParameters:
            try:
                client.branchBidParameters[bidParam] = float(client.branchBidParameters.get(bidParam))
            except ValueError as error:
                client.branchBidParameters[bidParam] = client.branchBidParameters.get(bidParam)

    return CLIENTS
