import datetime
import decimal
import json
import os
from boto3.dynamodb.conditions import Key
from utils import DynamoUtils
from utils.DecimalEncoder import DecimalEncoder

ONE_YEAR_IN_DAYS = 365

class Client:
    def __init__(
        self,
        orgId,
        clientName,
        emailAddresses,
        keyFilename, 
        pemFilename,
        bidParameters,
        adgroupBidParameters,
        branchBidParameters,
        appleCampaigns,
        keywordAdderParameters,
        branchIntegrationParameters,
        currency,
        appName,
        appID,
        campaignName
    ):  
        self._updatedBidsIsStale = False
        self._updatedAdgroupBidsIsStale = False
        
        # if "campaignId" not in keywordAdderIds or \
        #         "adGroupId" not in keywordAdderIds:
        #     raise NameError("Missing campaignId or adGroupId in keywordAdderIds")

        # kAPCI, kAPGI = keywordAdderIds["campaignId"], keywordAdderIds["adGroupId"]
        # if "search" not in kAPCI or "broad" not in kAPCI or "exact" not in kAPCI:
        #     raise NameError(
        #         "Missing search, broad, or exact in keywordAdderIds[\"campaignId\"]. It was %s." % str(kAPCI))

        # if "search" not in kAPGI or "broad" not in kAPGI or "exact" not in kAPGI:
        #     raise NameError(
        #         "Missing search, broad, or exact in keywordAdderIds[\"adGroupId\"]. It was %s." % str(kAPGI))

        self._orgId = orgId
        self._clientName = clientName
        self._emailAddresses = emailAddresses
        self._keyFilename = keyFilename
        self._pemFilename = pemFilename
        self._bidParameters = bidParameters
        self._adgroupBidParameters = adgroupBidParameters
        self._branchBidParameters = branchBidParameters
        self._appleCampaigns = appleCampaigns
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
        return self._adgroupBidParameters

    @adgroupBidParameters.setter
    def adgroupBidParameters(self, adgroupBidParameters):
        self._adgroupBidParameters = adgroupBidParameters

    @property
    def branchBidParameters(self):
        return dict(self._branchBidParameters)

    @branchBidParameters.setter
    def branchBidParameters(self, branchBidParameters):
        self._branchBidParameters = branchBidParameters   

    @property
    def appleCampaigns(self):
        return list(self._appleCampaigns)

    @property
    def keywordAdderParameters(self):
        return dict(self._keywordAdderParameters)

    @property
    def branchIntegrationParameters(self):
        return dict(self._branchIntegrationParameters)

    @property
    def currency(self):
        return self._currency

    # bid adjusters should use this method for cpi
    def calculateCPI(self, spend, installs):
        if int(installs) > 0:
            spend = "%s" % round(spend, 2)
            cpi = "%.2f" % round(float(spend) / float(installs), 2)
        else:
            spend = "%.2f" % 0
            cpi = "%.2f" % 0

        return cpi

    # and this for branch metrics
    def calculateBranchMetrics(self, spend, purchases, revenue):
        if int(purchases) > 0 and float(spend) > 0:   
            cpp = "%.2f" % round(float(spend) / float(purchases), 2)
        else:
            cpp = "%.2f" % 0

        if float(revenue) > 0 and float(spend) > 0:
            revenueOverCost = "%.2f" % round(float(revenue) / float(spend), 2)
            revenue = "%.2f" % round(revenue, 2)
        else:    
            revenueOverCost = "%.2f" % 0
            revenue = "%.2f" % 0

        return revenue, cpp, revenueOverCost

    # bid adjusters should use Client for CRUD operations against db
    def addRowToHistory(self, rowOfHistory, dynamoResource, end_date):
        self.addRowToCpiHistory(rowOfHistory, dynamoResource, end_date)
        self.addRowToCpiBranchHistory(rowOfHistory, dynamoResource, end_date)

    def addRowToCpiHistory(self, rowOfHistory, dynamoResource, end_date):
        table = dynamoResource.Table('cpi_history')
        
        org_id = str(self.orgId)  # TODO JF revisit when org_id is string
        timestamp = str(end_date) # write to history table with yesterday timestamp
        
        table.put_item(
            Item={
                'org_id': org_id,
                'timestamp': timestamp,
                'spend': rowOfHistory.get("spend"),
                'spend_exact': rowOfHistory.get("spend_exact"),
                'spend_search': rowOfHistory.get("spend_search"),
                'spend_broad': rowOfHistory.get("spend_broad"),
                'spend_brand': rowOfHistory.get("spend_brand"),
                'installs': rowOfHistory.get("installs"),
                'installs_exact': rowOfHistory.get("installs_exact"),
                'installs_search': rowOfHistory.get("installs_search"),
                'installs_broad': rowOfHistory.get("installs_broad"),
                'installs_brand': rowOfHistory.get("installs_brand"),
                'cpi': rowOfHistory.get("cpi"),
                'cpi_exact': rowOfHistory.get("cpi_exact"),
                'cpi_broad': rowOfHistory.get("cpi_broad"),
                'cpi_search': rowOfHistory.get("cpi_search"),
                'cpi_brand': rowOfHistory.get("cpi_brand")
            }
        )

    def addRowToCpiBranchHistory(self, rowOfHistory, dynamoResource, end_date):
        table = dynamoResource.Table('cpi_branch_history')
       
        org_id = str(self.orgId) # NOTE revisit when org_id is string
        timestamp = str(end_date) # write to history table with yesterday timestamp

        table.put_item(
            Item={
                'org_id': org_id,
                'timestamp': timestamp,
                'spend': rowOfHistory.get("spend"),
                'spend_exact': rowOfHistory.get("spend_exact"),
                'spend_search': rowOfHistory.get("spend_search"),
                'spend_broad': rowOfHistory.get("spend_broad"),
                'spend_brand': rowOfHistory.get("spend_brand"),
                'installs': rowOfHistory.get("installs"),
                'installs_exact': rowOfHistory.get("installs_exact"),
                'installs_search': rowOfHistory.get("installs_search"),
                'installs_broad': rowOfHistory.get("installs_broad"),
                'installs_brand': rowOfHistory.get("installs_brand"),
                'cpi': rowOfHistory.get("cpi"),
                'cpi_exact': rowOfHistory.get("cpi_exact"),
                'cpi_broad': rowOfHistory.get("cpi_broad"),
                'cpi_search': rowOfHistory.get("cpi_search"),
                'cpi_brand': rowOfHistory.get("cpi_brand"),
                'purchases': rowOfHistory.get("purchases"),
                'revenue': rowOfHistory.get("revenue"),
                'cpp': rowOfHistory.get("cpp"),
                'revenueOverCost': rowOfHistory.get("revenueOverCost")
            }
        )

    # gets all cpi history lines for a year
    def getYearOfHistory(self, dynamoResource):
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


    # NOTE currently unused, bid adjusters use getTotalCostPerInstallForCampaign
    # gets total cost per install for the lookback period
    # def getTotalCostPerInstall(self, dynamoResource, start_date, end_date, daysToLookBack):
    #     table = dynamoResource.Table('cpi_history')
    #     response = table.query(
    #         KeyConditionExpression=Key('org_id').eq(str(self.orgId)) & Key('timestamp').between(start_date.strftime(
    #         '%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
    #     )
    #     # default high so bid decreases arent done, until average is being computed from a long enough lookback period
    #     total_cost_per_install = 999999

    #     print("getTotalCostPerInstall:::orgId:::" + str(self.orgId))
    #     print("getTotalCostPerInstall:::start_date:::" + start_date.strftime('%Y-%m-%d'))
    #     print("getTotalCostPerInstall:::end_date:::" + end_date.strftime('%Y-%m-%d'))
    #     print("getTotalCostPerInstall:::daysToLookBack:::" + str(daysToLookBack))
    #     print("getTotalCostPerInstall:::dynamoResponse:::" + str(response))
        
    #     if len(response['Items']) >= daysToLookBack:
    #         totalCost, totalInstalls = 0.0, 0
    #         for i in response[u'Items']:
    #             totalCost += float(i['spend'])
    #             totalInstalls += int(i['installs'])
    #             if totalCost > 0 and totalInstalls > 0:
    #                 total_cost_per_install = totalCost / totalInstalls
    #     return total_cost_per_install


    # gets total cost per install for the lookback period
    def getTotalCostPerInstallForCampaign(self, dynamoResource, start_date, end_date, daysToLookBack, campaign):
         # default high so bid decreases arent done, until average is being computed from a long enough lookback period
        total_cost_per_install = 999999

        if campaign['campaignType'] == 'other':
            return total_cost_per_install # use default for other types

        table = dynamoResource.Table('cpi_history')
        response = table.query(
            KeyConditionExpression=Key('org_id').eq(str(self.orgId)) & Key('timestamp').between(start_date.strftime(
            '%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
        )
       
        print("getTotalCostPerInstall:::orgId:::" + str(self.orgId))
        print("getTotalCostPerInstall:::start_date:::" + start_date.strftime('%Y-%m-%d'))
        print("getTotalCostPerInstall:::end_date:::" + end_date.strftime('%Y-%m-%d'))
        print("getTotalCostPerInstall:::daysToLookBack:::" + str(daysToLookBack))
        print("getTotalCostPerInstall:::dynamoResponse:::" + str(response))
        
        installs_lookup_key = "installs_" + campaign["campaignType"]
        spend_lookup_key = "spend_" + campaign["campaignType"]

        print("installs_lookup_key " + str(installs_lookup_key))
        print("spend_lookup_key " + str(spend_lookup_key))

        if len(response['Items']) >= daysToLookBack:
            totalCost, totalInstalls = 0.0, 0
            for i in response[u'Items']:
                totalCost += float(i.get(spend_lookup_key,0.0))
                totalInstalls += int(i.get(installs_lookup_key,0))
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
                    client["orgDetails"]["appleCampaigns"],
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
                    continue
                else:
                    client.bidParameters[bidParam] = float(client.bidParameters.get(bidParam))

            for bidParam in client.adgroupBidParameters:
                if type(client.adgroupBidParameters[bidParam]) == str:
                    continue
                else:
                    client.adgroupBidParameters[bidParam] = float(client.adgroupBidParameters.get(bidParam))

            for bidParam in client.branchBidParameters:
                if type(client.branchBidParameters[bidParam]) == str:
                    continue
                else:
                    client.branchBidParameters[bidParam] = float(client.branchBidParameters.get(bidParam))

        return CLIENTS

# TODO implement
# if __name__ == "__main__":
    # Client.test()