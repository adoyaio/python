import datetime
import decimal
import json
import os
from boto3.dynamodb.conditions import Key
from utils import DynamoUtils

ONE_YEAR_IN_DAYS = 365

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

class Client:
    def __init__(self,
                 orgId,
                 clientName,
                 emailAddresses,
                 keyFilename, pemFilename,
                 bidParameters,
                 adgroupBidParameters,
                 branchBidParameters,
                 campaignIds,
                 keywordAdderIds,
                 keywordAdderParameters,
                 branchIntegrationParameters,
                 currency,
                 appName,
                 appID,
                 campaignName
                 ):
        
        self._updatedBidsIsStale = False
        self._updatedAdgroupBidsIsStale = False
        
        if "campaignId" not in keywordAdderIds or \
                "adGroupId" not in keywordAdderIds:
            raise NameError("Missing campaignId or adGroupId in keywordAdderIds")

        kAPCI, kAPGI = keywordAdderIds["campaignId"], keywordAdderIds["adGroupId"]
        if "search" not in kAPCI or "broad" not in kAPCI or "exact" not in kAPCI:
            raise NameError(
                "Missing search, broad, or exact in keywordAdderIds[\"campaignId\"]. It was %s." % str(kAPCI))

        if "search" not in kAPGI or "broad" not in kAPGI or "exact" not in kAPGI:
            raise NameError(
                "Missing search, broad, or exact in keywordAdderIds[\"adGroupId\"]. It was %s." % str(kAPGI))

        self._orgId = orgId
        self._clientName = clientName
        self._emailAddresses = emailAddresses
        self._keyFilename = keyFilename
        self._pemFilename = pemFilename
        self._bidParameters = bidParameters
        self._adgroupBidParameters = adgroupBidParameters
        self._branchBidParameters = branchBidParameters
        self._campaignIds = campaignIds
        self._keywordAdderIds = keywordAdderIds
        self._keywordAdderParameters = keywordAdderParameters
        self._branchIntegrationParameters = branchIntegrationParameters
        self._currency = currency
        self._appName = appName
        self._appID = appID
        self._campaignName = campaignName


    def __str__(self):
        return "Client '%s (#%d)" % (self.clientName, self.orgId)

    @property
    def orgId(self):
        return self._orgId

    @property
    def clientName(self):
        return self._clientName

    @property
    def emailAddresses(self):
        return list(self._emailAddresses)

    @property
    def keyFilename(self):
        return self._keyFilename

    @property
    def pemFilename(self):
        return self._pemFilename

    @property
    def bidParameters(self):
        return self._bidParameters

    @bidParameters.setter
    def bidParameters(self, bidParameters):
        self._bidParameters = bidParameters

    @property
    def adgroupBidParameters(self):
        return dict(self._adgroupBidParameters)

    @property
    def branchBidParameters(self):
        return dict(self._branchBidParameters)

    @property
    def keywordAdderIds(self):
        return dict(self._keywordAdderIds)

    @property
    def keywordAdderParameters(self):
        return dict(self._keywordAdderParameters)

    @property
    def campaignIds(self):
        return tuple(self._campaignIds)

    @property
    def branchIntegrationParameters(self):
        return dict(self._branchIntegrationParameters)

    @property
    def currency(self):
        return self._currency


    # bid adjusters should use Client for CRUD operations against db
    def addRowToHistory(self, stuff, dynamoResource, end_date):
        print("Client:::addRowToHistory:::stuff:::" + str(stuff))
        self.addRowToCpiHistory(stuff, dynamoResource, end_date)
        self.addRowToCpiBranchHistory(stuff, dynamoResource, end_date)

    def addRowToCpiHistory(self, stuff, dynamoResource, end_date):
        table = dynamoResource.Table('cpi_history')
        timestamp = str(end_date) # write to history table with yesterday timestamp
        spend = stuff[1]
        installs = int(stuff[2])
        cpi = stuff[3]
        org_id = str(self.orgId)  # TODO JF revisit when org_id is string
        print("Adding cpi_history line:", timestamp, spend, installs, cpi, org_id)
        table.put_item(
            Item={
                'timestamp': timestamp,
                'spend': spend,
                'installs': installs,
                'cpi': cpi,
                'org_id': org_id
            }
        )

    def addRowToCpiBranchHistory(self, stuff, dynamoResource, end_date):
        table = dynamoResource.Table('cpi_branch_history')

        # write to history table with yesterday timestamp
        timestamp = str(end_date)
        spend = stuff[1]
        installs = int(stuff[2])
        cpi = stuff[3]
        org_id = str(self.orgId) # TODO JF revisit when org_id is string
        purchases = int(stuff[4])
        revenue = stuff[5]
        cpp = stuff[6]
        revenueOverCost = stuff[7]
        print("Adding cpi_branch_history line:", timestamp, spend, installs, cpi, org_id, purchases, revenue, cpp, revenueOverCost)
        table.put_item(
            Item={
                'org_id': org_id,
                'timestamp': timestamp,
                'spend': spend,
                'installs': installs,
                'cpi': cpi,
                'purchases': purchases,
                'revenue': revenue,
                'cpp': cpp,
                'revenueOverCost': revenueOverCost
            }
        )

    # gets all cpi history lines for a year
    def getHistory(self, dynamoResource):
        today = datetime.date.today()
        end_date_delta = datetime.timedelta(days=1)
        start_date_delta = datetime.timedelta(ONE_YEAR_IN_DAYS)
        start_date = today - start_date_delta
        end_date = today - end_date_delta
        table = dynamoResource.Table('cpi_history')
        response = table.query(
            KeyConditionExpression=Key('org_id').eq(str(self.orgId)) & Key('timestamp').between(start_date.strftime(
                '%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        return response['Items']


    # gets total cost per install for the lookback period
    def getTotalCostPerInstall(self, dynamoResource, start_date, end_date, daysToLookBack):
        table = dynamoResource.Table('cpi_history')
        response = table.query(
            KeyConditionExpression=Key('org_id').eq(str(self.orgId)) & Key('timestamp').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
        # default high so bid decreases arent done, until average is being computed from a long enough lookback period
        total_cost_per_install = 999999

        print("getTotalCostPerInstall:::orgId:::" + str(self.orgId))
        print("getTotalCostPerInstall:::start_date:::" + start_date.strftime(
            '%Y-%m-%d'))
        print("getTotalCostPerInstall:::end_date:::" + end_date.strftime(
            '%Y-%m-%d'))
        print("getTotalCostPerInstall:::daysToLookBack:::" + str(daysToLookBack))
        print("getTotalCostPerInstall:::dynamoResponse:::" + str(response))
        
        if len(response['Items']) >= daysToLookBack:
            totalCost, totalInstalls = 0.0, 0
            for i in response[u'Items']:
                totalCost += float(i['spend'])
                totalInstalls += int(i['installs'])
                if totalCost > 0 and totalInstalls > 0:
                    total_cost_per_install = totalCost / totalInstalls
        return total_cost_per_install

    # gets total number of branch commerce events for lookback period
    def getTotalBranchEvents(self, dynamoResource, start_date, end_date):
        total_branch_events = 0
        total_branch_events = DynamoUtils.getBranchPurchasesForTimeperiod(
            dynamoResource, 
            self.orgId,
            start_date,
            end_date
        )
        return total_branch_events

    # gets total revenue for lookback period
    def getTotalBranchRevenue(self, dynamoResource, start_date, end_date):
        total_branch_revenue = 0.0
        total_branch_revenue = DynamoUtils.getBranchRevenueForTimeperiod(
            dynamoResource, 
            self.orgId,
            start_date,
            end_date
        )
        return total_branch_revenue

    # v0 opimization summary tables candidates for removal
    def writeUpdatedBids(self, dynamoResource, newValue):
        # print('Client.updatedBids: set value ' + str(newValue))
        # print('Client.updatedBids: is stale ' + str(self._updatedBidsIsStale))
        if not self._updatedBidsIsStale:
            self._updatedBidsIsStale = True
            # print('Client.updatedBids: set updatedBidsIsStale ' + str(self._updatedBidsIsStale))
        else:
            # print('Client.updatedBids: increment value ' + str(newValue) + ' + ' + self.readUpdatedBidsCount(dynamoResource))
            newValue = int(newValue) + int(self.readUpdatedBidsCount(dynamoResource))
        
        i = {
            "org_id": str(self.orgId),
            "bids": str(newValue)
        }
        # print("Client.updatedBids: adding bids entry:", i)
        table = dynamoResource.Table('bids')
        table.put_item(
            Item=i
        )

    def writeUpdatedAdgroupBids(self, dynamoResource, newValue):
        # print('Client.writeUpdatedAdgroupBids: set value ' + str(newValue))
        # print('Client.updatedAdgroupBidsIsStale: is stale ' + str(self._updatedAdgroupBidsIsStale))
        if not self._updatedAdgroupBidsIsStale:
            self._updatedAdgroupBidsIsStale = True
            # print('Client.updatedAdgroupBids: set updatedBidsIsStale ' + str(self._updatedAdgroupBidsIsStale))
        else:
            # print('Client.updatedAdgroupBids: increment value ' + str(newValue) + ' + ' + self.readUpdatedAdgroupBidsCount(dynamoResource))
            newValue = int(newValue) + int(self.readUpdatedAdgroupBidsCount(dynamoResource))

        i = {
            "org_id": str(self.orgId),
            "bids": str(newValue)
        }
        table = dynamoResource.Table('adgroup_bids')
        table.put_item(
            Item=i
        )

    def writePositiveKeywordsAdded(self, dynamoResource, newValue):
        item = {
            "org_id": str(self.orgId),
            "keywords": newValue
        }
        table = dynamoResource.Table('positive_keywords')
        table.put_item(
            Item=item
        )

    def writeNegativeKeywordsAdded(self, dynamoResource, newValue):
        item = {
            "org_id": str(self.orgId),
            "keywords": str(newValue)
        }
        table = dynamoResource.Table('negative_keywords')
        table.put_item(
            Item=item
        )

    def readUpdatedBidsCount(self, dynamoResource):
        table = dynamoResource.Table('bids')
        response = table.query(
            KeyConditionExpression=Key('org_id').eq(str(self.orgId))
        )

        if len(response['Items']) > 0:
            bids = response['Items'][0]["bids"]
        else:
            bids = 0
        return bids

    def readUpdatedAdgroupBidsCount(self, dynamoResource):
        table = dynamoResource.Table('adgroup_bids')
        response = table.query(
            KeyConditionExpression=Key('org_id').eq(str(self.orgId))
        )
        if len(response['Items']) > 0:
            bids = response['Items'][0]["bids"]
        else:
            bids = 0
        return bids

    def readPositiveKeywordsAdded(self, dynamoResource):
        table = dynamoResource.Table('positive_keywords')
        response = table.query(
            KeyConditionExpression=Key('org_id').eq(str(self.orgId))
        )
        if len(response['Items']) > 0:
            return response['Items'][0]["keywords"]

    def readNegativeKeywordsAdded(self, dynamoResource):
        table = dynamoResource.Table('negative_keywords')
        response = table.query(
            KeyConditionExpression=Key('org_id').eq(str(self.orgId))
        )
        if len(response['Items']) > 0:
            return response['Items'][0]["keywords"]


    # initialize and return array of Client objects
    def getClients(dynamoResource):
        CLIENTS = []
        for client in (dynamoResource.Table('clients').scan()["Items"]):
            CLIENTS.append(
                Client(
                    client["orgDetails"]["orgId"],
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
                )
            )

        # handle data types since dynamo uses decimal and bid adjusters use float
        for client in CLIENTS:
            for bidParam in client.bidParameters:
                if type(client.bidParameters[bidParam]) == str:
                    client.bidParameters[bidParam] = client.bidParameters.get(bidParam)
                else:
                    client.bidParameters[bidParam] = float(client.bidParameters.get(bidParam))

            for bidParam in client.adgroupBidParameters:
                if type(client.adgroupBidParameters[bidParam]) == str:
                    client.adgroupBidParameters[bidParam] = client.adgroupBidParameters.get(bidParam)
                else:
                    client.adgroupBidParameters[bidParam] = float(client.adgroupBidParameters.get(bidParam))

            for bidParam in client.branchBidParameters:
                if type(client.branchBidParameters[bidParam]) == str:
                    client.branchBidParameters[bidParam] = client.branchBidParameters.get(bidParam)
                else:
                    client.branchBidParameters[bidParam] = float(client.branchBidParameters.get(bidParam))

        return CLIENTS

# TODO implement
# if __name__ == "__main__":
    # Client.test()