from ast import Not
import datetime
import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key, Attr
# from decimal import *
# from decimal import Decimal
import logging
import decimal
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
    event_key = str(campaign_id) + dashG + str(ad_set_id) + dashG + keyword.lower().replace(" ", dashG)
    response = table.query(
        KeyConditionExpression=Key('branch_commerce_event_key').eq(event_key) & Key('timestamp').eq(timestamp),
    )
    return response

def getBranchCommerceEventsByCampaign(dynamoResource, campaign_id, timestamp):
    table = dynamoResource.Table('branch_commerce_events')
    keyExp = "Key('campaign_id').eq('" + str(campaign_id) + "') & Key('timestamp').eq('" + timestamp.strftime('%Y-%m-%d') + "')"
    query_kwargs = {}
    query_kwargs['IndexName'] = 'campaign_id-timestamp-index'
    query_kwargs['KeyConditionExpression']= eval(keyExp)
    response = table.query(**query_kwargs)
    return response

# updated to use org id
def getBranchPurchasesForTimeperiod(dynamoResource, org_id, start_date, end_date):
    table = dynamoResource.Table('branch_commerce_events')
    keyExp = "Key('org_id').eq('" + str(org_id) + "') & Key('timestamp').between('" + start_date.strftime('%Y-%m-%d') + "','"  + end_date.strftime('%Y-%m-%d') + "')"
    done = False
    start_key = None
    query_kwargs = {} 
    query_kwargs['KeyConditionExpression'] = eval(keyExp)
    query_kwargs['IndexName'] = 'org_id-timestamp-index'
    total_branch_events = 0
    while not done:
        if start_key:
            query_kwargs['ExclusiveStartKey'] = start_key
        response = table.query(**query_kwargs)
        for i in response.get('Items'):
            total_branch_events += int(i.get('count',0))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None 
     
    return total_branch_events

def getBranchRevenueForTimeperiod(dynamoResource, org_id, start_date, end_date):
    table = dynamoResource.Table('branch_commerce_events')
    keyExp = "Key('org_id').eq('" + str(org_id) + "') & Key('timestamp').between('" + start_date.strftime('%Y-%m-%d') + "','"  + end_date.strftime('%Y-%m-%d') + "')"
    done = False
    start_key = None
    query_kwargs = {} 
    query_kwargs['KeyConditionExpression'] = eval(keyExp)
    query_kwargs['IndexName'] = 'org_id-timestamp-index'
    total_branch_revenue = 0
    while not done:
        if start_key:
            query_kwargs['ExclusiveStartKey'] = start_key
        response = table.query(**query_kwargs)
        for i in response.get('Items'):
            total_branch_revenue += float(i.get('revenue',0))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None 
     
    return total_branch_revenue          

def getAppleKeywordData(dynamoResource, ad_group_id, start_date, end_date):
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    table = dynamoResource.Table('apple_keyword')
    keyExp = "Key('adgroup_id').eq('" + str(ad_group_id) + "') & Key('date').between('" + start_date + "','"  + end_date + "')"
    done = False
    start_key = None
    query_kwargs = {} 
    query_kwargs['KeyConditionExpression'] = eval(keyExp)
    query_kwargs['IndexName'] = 'adgroup_id-timestamp-index'
    returnVal = {}
    returnVal['Items'] = []
    returnVal['Count'] = 0
    while not done:
        if start_key:
            query_kwargs['ExclusiveStartKey'] = start_key
        response = table.query(**query_kwargs)
        returnVal['Items'].extend(response.get('Items'))
        returnVal['Count'] = response.get('Count') + returnVal['Count']
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None 
    return returnVal


# cast to int here because client table was migrated from client.json 
# NOTE in dynamo numbers are serialized so there is no advantage to using int 
# consider using string for consistency

# def getClient(dynamoResource, org_id):
#     from Client import Client
#     table = dynamoResource.Table('clients')
#     response = table.query(
#         KeyConditionExpression=Key('orgId').eq(int(org_id))
#     )
#     clientDict = response['Items'][0]
#     parsed = json.loads(json.dumps(clientDict['orgDetails'],cls=DecimalEncoder))
#     client = Client.buildFromDictionary(parsed)
#     return client

# updated to use string orgId
def getClient(dynamoResource, org_id):
    from Client import Client
    table = dynamoResource.Table('clients_2')
    response = table.query(
        KeyConditionExpression=Key('orgId').eq(str(org_id))
    )
    clientDict = response['Items'][0]

    # TODO remove this and return dynamo response as is
    parsed = json.loads(json.dumps(clientDict['orgDetails'],cls=DecimalEncoder))
    client = Client.buildFromDictionary(parsed)
    return client

def getClients(dynamoResource, org_id):
    from Client import Client
    table = dynamoResource.Table('clients_2')
    response = table.scan(
        FilterExpression=Attr('orgId').begins_with(str(org_id))
    )
    # response = table.query(Key('orgId').begins_with(str(org_id)))
    print(str(response))
    clients = response.get('Items', [])    
    return clients


def getClientHistory(dynamoResource, org_id):
    today = datetime.date.today()
    end_date_delta = datetime.timedelta(days=1)
    start_date_delta = datetime.timedelta(365)
    start_date = today - start_date_delta
    end_date = today - end_date_delta

    table = dynamoResource.Table('cpi_history')
    response = table.query(
        KeyConditionExpression=Key('org_id').eq(org_id & Key('timestamp').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        ))
    return response['Items']

# TODO non branch history
# def getClientHistoryByTime(dynamoResource, client_id, start_date, end_date):
#     table = dynamoResource.Table('cpi_history')
#     response = table.query(
#         KeyConditionExpression=Key('org_id').eq(client_id) & Key('timestamp').between(end_date, start_date),
#     )
#     return response['Items']


# def getClientHistoryNumRecs(dynamoResource, org_id, total_recs):
#     table = dynamoResource.Table('cpi_history')
#     response = table.query(
#         KeyConditionExpression=Key('org_id').eq(org_id),
#         ScanIndexForward=False,
#         Limit=int(total_recs)
#     )
#     return response['Items']


def getClientBranchHistoryByTime(
        dynamoResource, 
        org_id, 
        total_recs, 
        offset, 
        start_date, 
        end_date
        ):
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("getClientBranchHistoryByTime")
    logger.info("offset " + str(offset)) 
    logger.info('start date ' + start_date)
    logger.info('end date ' + end_date)

    table = dynamoResource.Table('cpi_branch_history')
    
    # build the KeyConditionExpression
    if start_date == 'all':
        keyExp = "Key('org_id').eq('" + org_id + "')"
    else:
        keyExp = "Key('org_id').eq('" + org_id + "') & Key('timestamp').between('" + end_date + "','"  + start_date + "')"
    
    logger.info("getClientBranchHistoryByTime:::keyExp" + keyExp)

    # first page: dont send ExclusiveStartKey
    if offset.get("org_id") == "init":
        logger.info("first page init offset")

        done = False
        start_key = None
        query_kwargs = {} 
        query_kwargs['KeyConditionExpression'] = eval(keyExp)
        query_kwargs['ScanIndexForward']=False
        returnVal = []

        while not done:
            if start_key:
                query_kwargs['ExclusiveStartKey'] = start_key
            response = table.query(**query_kwargs)
            returnVal.extend(response.get('Items'))
            start_key = response.get('LastEvaluatedKey', None)
            # will ignore total recs for now, doesn't work well with the data viz
            # done = len(returnVal) >= int(total_recs) or (start_key is None)
            done = start_key is None
    
        # logger.info("response:::" + str(json.dumps(response, cls=DecimalEncoder, indent=2)))

        count = table.query(
            Select="COUNT",
            KeyConditionExpression=eval(keyExp)
        )
    # for now server side pagination is not used so this block will not run
    else:
        response = table.query(
            KeyConditionExpression=eval(keyExp),
            Limit=int(total_recs),
            ExclusiveStartKey=offset,
            ScanIndexForward=False
        )

        returnVal = response.get('Items')

        # calc total for pagingation
        count = table.query(
            Select="COUNT",
            KeyConditionExpression=eval(keyExp),
        )

        logger.info("count:::" + str(json.dumps(count.get('Count',0), cls=DecimalEncoder, indent=2)))
        logger.info("count response:::" + str(json.dumps(count, cls=DecimalEncoder, indent=2)))

    # determine whether next page exists and send response
    try:
        nextOffset =  response['LastEvaluatedKey']
        print("nextOffset:::" + str(nextOffset))
    except KeyError as error:
        nextOffset = {
            'timestamp': '',
            'org_id': ''
        }
    return { 
            'history': returnVal, 
            'offset': nextOffset,
            'count': count.get('Count',0)
        }



def getCampaignBranchHistoryByTime(
        dynamoResource, 
        campaign_ids, 
        org_id,
        total_recs, 
        offset, 
        start_date, 
        end_date
        ):

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("getCampaignBranchHistoryByTime")

    logger.info('offset' + str(offset))
    if total_recs != None:
        logger.info('total_recs' + str(total_recs))
    logger.info('campaign_ids' + str(campaign_ids))

    table = dynamoResource.Table('campaign_branch_history')

    # build the KeyConditionExpression
    if start_date == 'all':
        keyExp = "Key('org_id').eq('" + org_id + "')"
    else:
        keyExp = "Key('org_id').eq('" + org_id + "') & Key('timestamp').between('" + end_date + "','"  + start_date + "')"
    
    # build the FilterExpression
    filterExp = "Attr('campaign_id').is_in(campaign_ids)"
    logger.info("keyExp" + keyExp)

    # first page: dont send ExclusiveStartKey
    if offset.get("campaign_id") == "init":
        logger.info("first page init offset")

        # apply filter expression if needed
        logger.info("filter expression > 0 len::" + filterExp)
                    
        done = False
        start_key = None
        query_kwargs = {} 
        query_kwargs['KeyConditionExpression'] = eval(keyExp)
        query_kwargs['FilterExpression'] = eval(filterExp)
        query_kwargs['IndexName'] = 'org_id-timestamp-index'
        query_kwargs['ScanIndexForward']=False
        returnVal = []
        while not done:
            if start_key:
                query_kwargs['ExclusiveStartKey'] = start_key
            response = table.query(**query_kwargs)
            returnVal.extend(response.get('Items'))
            start_key = response.get('LastEvaluatedKey', None)
            # will ignore total recs for now, doesn't work well with the data viz
            # done = len(returnVal) >= int(total_recs) or (start_key is None)
            done = start_key is None
        
        # hack for dynamo paging and filtering to work together
        try:
            # last = returnVal[int(total_recs)] # pull the last record of the data set we want to send back
            last = returnVal[-1]
            org_id = last.get('org_id')
            timestamp = last.get('timestamp')
            campaign_id = last.get('campaign_id')
            response['LastEvaluatedKey'] = { 'org_id':org_id, 'timestamp':timestamp, 'campaign_id': campaign_id}
        except:
            logger.info("no last eval key")
        
        # returnVal = returnVal[0:int(total_recs)-1]
        done = False
        start_key = None
        query_kwargs = {} 
        query_kwargs['KeyConditionExpression'] = eval(keyExp)
        query_kwargs['FilterExpression'] = eval(filterExp)
        query_kwargs['IndexName'] = 'org_id-timestamp-index'
        query_kwargs['Select'] = 'COUNT'
        query_kwargs['ScanIndexForward']=False
        count = 0
        while not done:
            if start_key:
                query_kwargs['ExclusiveStartKey'] = start_key
            count_response = table.query(**query_kwargs)
            count = count + count_response['Count']
            start_key = count_response.get('LastEvaluatedKey', None)
            done = start_key is None
        logger.info("count:::" + str(count))

        # determine whether next page exists and send response
        try:
            nextOffset =  response['LastEvaluatedKey']
            print("nextOffset:::" + str(nextOffset))
        except KeyError as error:
            nextOffset = {
                'timestamp': '',
                'campaign_id': '',
                'org_id': ''
            }
        return { 
                'history': returnVal, 
                'offset': nextOffset,
                'count': count
                }

    # gt first page: send ExclusiveStartKey
    else: 
        logger.info("next page offset" + str(offset))

        # apply filter expression if needed
        done = False
        start_key = offset
        query_kwargs = {} 
        query_kwargs['KeyConditionExpression'] = eval(keyExp)
        query_kwargs['FilterExpression'] = eval(filterExp)
        query_kwargs['IndexName'] = 'org_id-timestamp-index'
        query_kwargs['ScanIndexForward']=False
        returnVal = []
        while not done:
            if start_key:
                query_kwargs['ExclusiveStartKey'] = start_key
            response = table.query(**query_kwargs)
            returnVal.extend(response.get('Items'))
            start_key = response.get('LastEvaluatedKey', None)
            # done = len(returnVal) >= int(total_recs) or (start_key is None)
            done = start_key is None
        # hack 
        try:
            # last = returnVal[int(total_recs)]
            last = returnVal[-1]
            org_id = last.get('org_id')
            timestamp = last.get('timestamp')
            campaign_id = last.get('campaign_id')
            response['LastEvaluatedKey'] = { 'org_id':org_id, 'timestamp':timestamp, 'campaign_id': campaign_id}
        except:
            logger.info("no last eval key")
        
        # returnVal = returnVal[0:int(total_recs)-1] 
        # calc total for pagingation
        done = False
        start_key = None
        query_kwargs = {} 
        query_kwargs['KeyConditionExpression'] = eval(keyExp)
        query_kwargs['FilterExpression'] = eval(filterExp)
        query_kwargs['IndexName'] = 'org_id-timestamp-index'
        query_kwargs['Select'] = 'COUNT'
        query_kwargs['ScanIndexForward']=False
        count = 0
        while not done:
            if start_key:
                query_kwargs['ExclusiveStartKey'] = start_key
            count_response = table.query(**query_kwargs)
            count = count + count_response['Count']
            start_key = count_response.get('LastEvaluatedKey', None)
            done = start_key is None
        logger.info("count:::" + str(count))

        # determine whether next page exists and send response
        try:
            nextOffset =  response['LastEvaluatedKey']
            print("nextOffset:::" + str(nextOffset))
        except KeyError as error:
            nextOffset = {
                'timestamp': '',
                'campaign_id': '',
                'org_id': ''
            }
        return { 
                'history': returnVal, 
                'offset': nextOffset,
                'count': count
                }

# def getCampaignBranchHistoryByTime(
#         dynamoResource, 
#         campaign_ids, 
#         org_id,
#         total_recs, 
#         offset, 
#         start_date, 
#         end_date
#         ):
    
#     logger = logging.getLogger()
#     logger.setLevel(logging.INFO)
#     logger.info("getCampaignBranchHistoryByTime")

#     logger.info('offset' + str(offset))
#     logger.info('total_recs' + str(total_recs))
#     logger.info('campaign_ids' + str(campaign_ids))

#     table = dynamoResource.Table('campaign_branch_history')
    
#     # build the KeyConditionExpression
#     if start_date == 'all':
#         keyExp = "Key('org_id').eq('" + org_id + "')"
#     else:
#         keyExp = "Key('org_id').eq('" + org_id + "') & Key('timestamp').between('" + end_date + "','"  + start_date + "')"
    
#     logger.info("getClientBranchHistoryByTime:::keyExp" + keyExp)

#     # first page: dont send ExclusiveStartKey
#     if offset.get("campaign_id") == "init":
#         logger.info("first page init offset")
#         response = table.query(
#             KeyConditionExpression=eval(keyExp),
#             Limit=int(total_recs),
#             ScanIndexForward=False
#         )

#         returnVal = response.get('Items')
#         logger.info("response:::" + str(json.dumps(response, cls=DecimalEncoder, indent=2)))

#         count = table.query(
#             Select="COUNT",
#             KeyConditionExpression=eval(keyExp),
#         )   
#     else:
#         response = table.query(
#             KeyConditionExpression=eval(keyExp),
#             Limit=int(total_recs),
#             ExclusiveStartKey=offset,
#             ScanIndexForward=False
#         )

#         returnVal = response.get('Items')
#         # logger.info("response:::" + str(json.dumps(response, cls=DecimalEncoder, indent=2)))

#         # calc total for pagingation
#         count = table.query(
#             Select="COUNT",
#             KeyConditionExpression=eval(keyExp),
#         )

#         logger.info("count:::" + str(json.dumps(count.get('Count',0), cls=DecimalEncoder, indent=2)))
#         logger.info("count response:::" + str(json.dumps(count, cls=DecimalEncoder, indent=2)))

#     # determine whether next page exists and send response
#     try:
#         nextOffset =  response['LastEvaluatedKey']
#         print("nextOffset:::" + str(nextOffset))
#     except KeyError as error:
#         nextOffset = {
#             'timestamp': '',
#             'campaign_id': ''
#         }
#     return { 
#             'history': returnVal, 
#             'offset': nextOffset,
#             'count': count.get('Count',0)
#         }

def getClientBranchHistory(dynamoResource, org_id, total_recs):
    table = dynamoResource.Table('cpi_branch_history')
    response = table.query(
        KeyConditionExpression=Key('org_id').eq(org_id),
        ScanIndexForward=True,
        Limit=int(total_recs)
    )
    return response['Items']

# # TODO swap for apple_branch_keyword when available
# def getClientKeywordHistory(dynamoResource, org_id, total_recs, offset):
#     table = dynamoResource.Table('apple_keyword')
    
#     # first page dont send ExclusiveStartKey
#     if offset.get("keyword_id") == "init":
#         response = table.query(
#             KeyConditionExpression=Key('org_id').eq(org_id),
#             IndexName='org_id-timestamp-index',
#             ScanIndexForward=False,
#             Limit=int(total_recs),
#         )
#     # not first page send ExclusiveStartKey
#     else: 
#         response = table.query(
#             KeyConditionExpression=Key('org_id').eq(org_id),
#             IndexName='org_id-timestamp-index',
#             ScanIndexForward=False,
#             Limit=int(total_recs),
#             ExclusiveStartKey = offset,   
#         )
    
#     # calc total for pagingation
#     count = table.query(
#         Select="COUNT",
#         KeyConditionExpression=Key('org_id').eq(org_id),
#         IndexName='org_id-timestamp-index',  
#     )

#     # determine whether next page exists and send response
#     try:
#         nextOffset =  response['LastEvaluatedKey']
#     except KeyError as error:
#         nextOffset = {
#             'date': '',
#             'keyword_id': '',
#             'org_id': ''
#         }
#     return { 
#             'history': response['Items'], 
#             'offset': nextOffset,
#             'count': count['Count']
#             }
    
def getClientKeywordHistory(
        dynamoResource, 
        org_id, 
        total_recs, 
        offset, 
        start_date, 
        end_date, 
        matchType,
        keywordStatus
        ):

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.info("getClientKeywordHistory")

    table = dynamoResource.Table('apple_keyword')

    # build the KeyConditionExpression
    if start_date == 'all':
        keyExp = "Key('org_id').eq('" + org_id + "')"
    else:
        keyExp = "Key('org_id').eq('" + org_id + "') & Key('date').between('" + end_date + "','"  + start_date + "')"
    
    # build the FilterExpression
    filterExp = "Attr('local_spend').gt(0)"
    if matchType != 'all' and keywordStatus != 'all':
        filterExp = filterExp + " & Attr('keywordStatus').eq(keywordStatus) & Attr('matchType').eq(matchType)"
    elif keywordStatus != 'all':
        filterExp = filterExp + " & Attr('keywordStatus').eq(keywordStatus)"
    elif matchType != 'all':
        filterExp = filterExp + " & Attr('matchType').eq(matchType)"
       
    logger.info("getClientKeywordHistory:::filterExp" + filterExp)
    logger.info("getClientKeywordHistory:::keyExp" + keyExp)

    # first page: dont send ExclusiveStartKey
    if offset.get("keyword_id") == "init":
        logger.info("first page init offset")

        # apply filter expression if needed
        if len(filterExp) > 0:
            logger.info("filter expression > 0 len::" + filterExp)
                        
            done = False
            start_key = None
            query_kwargs = {} 
            query_kwargs['KeyConditionExpression'] = eval(keyExp)
            query_kwargs['FilterExpression'] = eval(filterExp)
            query_kwargs['IndexName'] = 'org_id-timestamp-index'
            returnVal = []

            while not done:
                if start_key:
                    query_kwargs['ExclusiveStartKey'] = start_key
                response = table.query(**query_kwargs)
                returnVal.extend(response.get('Items'))
                start_key = response.get('LastEvaluatedKey', None)
                done = (start_key is None)
                # done = len(returnVal) >= int(total_recs) or (start_key is None)
            
            # hack for dynamo paging and filtering to work together
            # try:
            #     last = returnVal[int(total_recs)] # pull the last record of the data set we want to send back
            #     org_id = last.get('org_id')
            #     date = last.get('date')
            #     keyword_id = last.get('keyword_id')
            #     response['LastEvaluatedKey'] = { 'org_id':org_id, 'date':date, 'keyword_id': keyword_id}
            # except:
            #     logger.info("no last eval key")
            
            # returnVal = returnVal[0:int(total_recs)-1]
        

            done = False
            start_key = None
            query_kwargs = {} 
            query_kwargs['KeyConditionExpression'] = eval(keyExp)
            query_kwargs['FilterExpression'] = eval(filterExp)
            query_kwargs['IndexName'] = 'org_id-timestamp-index'
            query_kwargs['Select'] = 'COUNT'
            count = 0

            while not done:
                if start_key:
                    query_kwargs['ExclusiveStartKey'] = start_key
                count_response = table.query(**query_kwargs)
                count = count + count_response['Count']
                start_key = count_response.get('LastEvaluatedKey', None)
                done = start_key is None

            logger.info("count:::" + str(count))
        
        # no filter TODO used?
        else:
            logger.info("filter expression = 0 len" + filterExp)
            response = table.query(
                KeyConditionExpression=eval(keyExp),
                IndexName='org_id-timestamp-index',
                Limit=int(total_recs)
            )

            returnVal = response.get('Items')
            logger.info("response:::" + str(json.dumps(response, cls=DecimalEncoder, indent=2)))

            count = table.query(
                Select="COUNT",
                KeyConditionExpression=eval(keyExp),
                IndexName='org_id-timestamp-index'
            )

    # gt first page: send ExclusiveStartKey
    else: 
        logger.info("next page offset" + str(offset))

        # apply filter expression if needed
        if len(filterExp) > 0:
            done = False
            start_key = offset
            query_kwargs = {} 
            query_kwargs['KeyConditionExpression'] = eval(keyExp)
            query_kwargs['FilterExpression'] = eval(filterExp)
            query_kwargs['IndexName'] = 'org_id-timestamp-index'
            returnVal = []

            while not done:
                if start_key:
                    query_kwargs['ExclusiveStartKey'] = start_key
                response = table.query(**query_kwargs)
                returnVal.extend(response.get('Items'))
                start_key = response.get('LastEvaluatedKey', None)
                done = len(returnVal) >= int(total_recs) or (start_key is None)

            # hack 
            try:
                last = returnVal[int(total_recs)]
                org_id = last.get('org_id')
                date = last.get('date')
                keyword_id = last.get('keyword_id')
                response['LastEvaluatedKey'] = { 'org_id':org_id, 'date':date, 'keyword_id': keyword_id}
            except:
                logger.info("no last eval key")
            
            returnVal = returnVal[0:int(total_recs)-1] 

            # calc total for pagingation
            done = False
            start_key = None
            query_kwargs = {} 
            query_kwargs['KeyConditionExpression'] = eval(keyExp)
            query_kwargs['FilterExpression'] = eval(filterExp)
            query_kwargs['IndexName'] = 'org_id-timestamp-index'
            query_kwargs['Select'] = 'COUNT'
            count = 0

            while not done:
                if start_key:
                    query_kwargs['ExclusiveStartKey'] = start_key
                count_response = table.query(**query_kwargs)
                count = count + count_response['Count']
                start_key = count_response.get('LastEvaluatedKey', None)
                done = start_key is None

            logger.info("count:::" + str(count))

        # no filter TODO used?
        else:
            response = table.query(
                KeyConditionExpression=eval(keyExp),
                IndexName='org_id-timestamp-index',
                Limit=int(total_recs),
                ExclusiveStartKey=offset
            )

            returnVal = response.get('Items')

            # calc total for pagingation
            count = table.query(
                Select="COUNT",
                KeyConditionExpression=eval(keyExp),
                IndexName='org_id-timestamp-index'
            )

            logger.info("count:::" + str(json.dumps(count.get('Count',0), cls=DecimalEncoder, indent=2)))
            logger.info("count response:::" + str(json.dumps(count, cls=DecimalEncoder, indent=2)))

    # determine whether next page exists and send response
    try:
        nextOffset =  response['LastEvaluatedKey']
        print("nextOffset:::" + str(nextOffset))
    except KeyError as error:
        nextOffset = {
            'date': '',
            'keyword_id': '',
            'org_id': ''
        }
    return { 
            'history': returnVal, 
            'offset': nextOffset,
            'count': count
            }