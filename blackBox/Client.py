import datetime
import decimal
import json
import os

import boto3
from boto3 import dynamodb
from boto3.dynamodb.conditions import Key

from utils import DynamoUtils

DATA_DIR = "data"
CERT_DIR = "cert"
CLIENTS_DATA_FILENAME = "clients.json"
CLIENT_UPDATED_BIDS_FILENAME_TEMPLATE = "bids_%s.json"
CLIENT_UPDATED_ADGROUP_BIDS_FILENAME_TEMPLATE = "adgroup_bids_%s.json"
CLIENT_POSITIVE_KEYWORDS_FILENAME_TEMPLATE = "positive_keywords_%s.json"
CLIENT_NEGATIVE_KEYWORDS_FILENAME_TEMPLATE = "negative_keywords_%s.json"
CLIENT_HISTORY_FILENAME_TEMPLATE = "history_%s.csv"
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
        self.appName = appName
        self.appID = appID
        self.campaignName = campaignName
        # The history data is populated when requested.

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

    # TODO JF rework this, don't use filesystem
    @property
    def keyPathname(self):
        return os.path.join(CERT_DIR, self._keyFilename)

    # TODO JF rework this, don't use filesystem
    @property
    def pemPathname(self):
        return os.path.join(CERT_DIR, self._pemFilename)

    @property
    def bidParameters(self):
        #return dict(self._bidParameters)
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
        return dict(self._keywordAdderIds)  # TODO: Deep copy. --DS, 14-Sep-2018

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


    def updatedBids(self, dynamoResource, newValue):
        print('Client.updatedBids: set value ' + str(newValue))
        print('Client.updatedBids: is stale ' + str(self._updatedBidsIsStale))

        if not self._updatedBidsIsStale:
            self._updatedBidsIsStale = True
            print('Client.updatedBids: set updatedBidsIsStale ' + str(self._updatedBidsIsStale))
        else:
            print('Client.updatedBids: increment value ' + str(newValue) + ' + ' + self.readUpdatedBidsCount(dynamoResource))
            newValue = int(newValue) + int(self.readUpdatedBidsCount(dynamoResource))

        item = {
            "org_id": str(self.orgId),
            "bids": str(newValue)
        }

        # v1 add dynamo db call
        print("Client.updatedBids: adding bids entry:", item)
        table = dynamoResource.Table('bids')
        table.put_item(
            Item=item
        )

    def updatedAdgroupBids(self, dynamoResource, newValue):
        print('Client.updatedAdgroupBids: set value ' + str(newValue))
        print('Client.updatedAdgroupBidsIsStale: is stale ' + str(self._updatedAdgroupBidsIsStale))

        if not self._updatedAdgroupBidsIsStale:
            self._updatedAdgroupBidsIsStale = True
            print('Client.updatedAdgroupBids: set updatedBidsIsStale ' + str(self._updatedAdgroupBidsIsStale))
        else:
            print('Client.updatedAdgroupBids: increment value ' + str(newValue) + ' + ' + self.readUpdatedAdgroupBidsCount(dynamoResource))
            newValue = int(newValue) + int(self.readUpdatedAdgroupBidsCount(dynamoResource))

        item = {
            "org_id": str(self.orgId),
            "bids": str(newValue)
        }

        # v1 add dynamo db call
        print("Client.updatedAdgroupBids: adding bids entry:", item)
        table = dynamoResource.Table('adgroup_bids')
        table.put_item(
            Item=item
        )

    def positiveKeywordsAdded(self, dynamoResource, newValue):
        print('Client.positiveKeywordsAdded: set value ' + str(newValue));
        item = {
            "org_id": str(self.orgId),
            "keywords": newValue
        }

        # v1 add dynamo db call
        print("Client.positiveKeywordsAdded: adding bids entry:", item)
        table = dynamoResource.Table('positive_keywords')
        table.put_item(
            Item=item
        )

    def negativeKeywordsAdded(self, dynamoResource, newValue):
        print('Client.positiveKeywordsAdded: set value ' + str(newValue));
        item = {
            "org_id": str(self.orgId),
            "keywords": str(newValue)
        }

        # v1 add dynamo db call
        print("Client.negativeKeywordsAdded: adding bids entry:", item)
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

    # ^  # ----------------------------------------------------------------------------
    # ^  @property
    # ^  @debug
    # ^  def campaignIds(self):
    # ^    today = datetime.date.today()
    # ^
    # ^    payload = {
    # ^                "startTime"                  : str(today),
    # ^                "endTime"                    : str(today),
    # ^                "returnRowTotals"            : True,
    # ^                "returnRecordsWithNoMetrics" : True,
    # ^                "selector" : {
    # ^                  "orderBy"    : [ { "field" : "localSpend", "sortOrder" : "DESCENDING" } ],
    # ^                  "fields"     : [ "localSpend", "taps", "impressions", "conversions", "avgCPA", "avgCPT", "ttr", "conversionRate" ],
    # ^                  "pagination" : { "offset" : 0, "limit" : 1000 }
    # ^                },
    # ^                #"groupBy"                    : [ "COUNTRY_CODE" ],
    # ^                #"granularity"                : 2, # 1 is hourly, 2 is daily, 3 is monthly etc
    # ^              }
    # ^
    # ^    headers = { "Authorization": "orgId=%s" % self.orgId }
    # ^
    # ^    dprint("Headers: %s\n" % headers)
    # ^    dprint("Payload: %s\n" % payload)
    # ^    #dprint("Apple 'Get Reports' URL: %s\n" % GET_REPORTS_URL)
    # ^
    # ^    response = requests.post(Client._GET_REPORTS_URL,
    # ^                             cert=(self.pemPathname, self.keyPathname),
    # ^                             json=payload,
    # ^                             headers=headers)
    # ^
    # ^    dprint("Response: '%s'" % response)
    # ^
    # ^    return [ item["metadata"]["campaignId"] for item in json.loads(response.text)["data"]["reportingDataResponse"]["row"] ]

    # V1 code to use dynamo
    # ----------------------------------------------------------------------------
    def addRowToHistory(self, stuff, dynamoResource):
        print("stuff:", stuff)
        table = dynamoResource.Table('cpi_history')

        timestamp = stuff[0]
        spend = stuff[1]
        installs = int(stuff[2])
        cpi = stuff[3]
        org_id = str(self.orgId)
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

        total_cost_per_install = 0

        if len(response['Items']) >= daysToLookBack:
            totalCost, totalInstalls = 0.0, 0
            for i in response[u'Items']:
                totalCost += float(i['spend'])
                totalInstalls += int(i['installs'])
                print(json.dumps(i, cls=DecimalEncoder))

                if totalCost > 0 and totalInstalls > 0:
                    total_cost_per_install = totalCost / totalInstalls

        return total_cost_per_install

    # gets total number of branch commerce events for lookback period
    def getTotalBranchEvents(self, dynamoResource, start_date, end_date):
        total_branch_events = 0
        for id in self.campaignIds:
            print("Client:::getTotalBranchEvents:::" + str(id))
            total_branch_events = total_branch_events + DynamoUtils.getBranchPurchasesForTimeperiod(dynamoResource, id, start_date, end_date)

        return total_branch_events

    # gets total revenue for lookback period
    def getTotalBranchRevenue(self, dynamoResource, start_date, end_date):
        total_branch_revenue = 0.0
        for id in self.campaignIds:
            print("Client:::getTotalBranchRevenue:::" + str(id))
            total_branch_revenue = total_branch_revenue + DynamoUtils.getBranchRevenueForTimeperiod(dynamoResource, id, start_date, end_date)

        return total_branch_revenue

    # ----------------------------------------------------------------------------
    @staticmethod
    def test():
        client = Client("Org_123",
                        "Clientname_123",
                        ("Email_Address_1", "Email_Address_2"),
                        "Key_filename_123",
                        "Pem_filename_123",
                        {
                            "STALE_RAISE_IMPRESSION_THRESH": 11,
                            "TAP_THRESHOLD": 22,
                            "HIGH_CPI_BID_DECREASE_THRESH": 44,
                            "NO_INSTALL_BID_DECREASE_THRESH": 55,
                            "STALE_RAISE_BID_BOOST": 66,
                            "LOW_CPA_BID_BOOST": 77,
                            "HIGH_CPA_BID_DECREASE": 88,
                            "MAX_BID": 99,
                            "MIN_BID": 111,
                        },
                        {
                            "STALE_RAISE_IMPRESSION_THRESH": 111,
                            "TAP_THRESHOLD": 222,
                            "LOW_CPI_BID_INCREASE_THRESH": 333,
                            "HIGH_CPI_BID_DECREASE_THRESH": 444,
                            "NO_INSTALL_BID_DECREASE_THRESH": 555,
                            "STALE_RAISE_BID_BOOST": 666,
                            "LOW_CPA_BID_BOOST": 777,
                            "HIGH_CPA_BID_DECREASE": 888,
                            "MAX_BID": 999,
                            "MIN_BID": 1111,
                        },
                        (123, 456),
                        {"campaignId": {"search": "searchCampaignId",
                                        "broad": "broadCampaignId",
                                        "exact": "exactCampaignId",
                                        },
                         "adGroupId": {"search": "searchGroupId",
                                       "broad": "broadGroupId",
                                       "exact": "exactGroupId",
                                       },
                         },
                        {
                            "NEGATIVE_KEYWORD_TAP_THRESHOLD": -1001,
                            "NEGATIVE_KEYWORD_CONVERSION_THRESHOLD": -1002,
                            "TARGETED_KEYWORD_TAP_THRESHOLD": -1003,
                            "TARGETED_KEYWORD_CONVERSION_THRESHOLD": -1004,
                            "EXACT_MATCH_DEFAULT_BID": -1005,
                            "BROAD_MATCH_DEFAULT_BID": -1006,
                        }
                        )
        if client.orgId != "Org_123":
            raise ZeroDivisionError("Failed orgId.")

        if client.clientName != "Clientname_123":
            raise ZeroDivisionError("Failed clientName.")

        if client.emailAddresses != ["Email_Address_1", "Email_Address_2"]:
            raise ZeroDivisionError("Failed emailAddresses.")

        if client.keyPathname != "/home/scott/ScottKaplan/Certificates/Key_filename_123":
            raise ZeroDivisionError("Failed keyPathname.")

        if client.pemPathname != "/home/scott/ScottKaplan/Certificates/Pem_filename_123":
            raise ZeroDivisionError("Failed pemPathname.")

        BP = client.bidParameters
        if BP["STALE_RAISE_IMPRESSION_THRESH"] != 11 or \
                BP["TAP_THRESHOLD"] != 22 or \
                BP["HIGH_CPI_BID_DECREASE_THRESH"] != 44 or \
                BP["NO_INSTALL_BID_DECREASE_THRESH"] != 55 or \
                BP["STALE_RAISE_BID_BOOST"] != 66 or \
                BP["LOW_CPA_BID_BOOST"] != 77 or \
                BP["HIGH_CPA_BID_DECREASE"] != 88 or \
                BP["MAX_BID"] != 99 or \
                BP["MIN_BID"] != 111:
            raise ZeroDivisionError("Failed bidParameters test: %s." % str(BP))

        copyOfBP = dict(BP)
        for i in ("STALE_RAISE_IMPRESSION_THRESH",
                  "TAP_THRESHOLD",
                  "HIGH_CPI_BID_DECREASE_THRESH",
                  "NO_INSTALL_BID_DECREASE_THRESH",
                  "STALE_RAISE_BID_BOOST",
                  "LOW_CPA_BID_BOOST",
                  "HIGH_CPA_BID_DECREASE",
                  "MAX_BID",
                  "MIN_BID"):
            del copyOfBP[i]

        if len(copyOfBP) != 0:
            raise ZeroDivisionError("Failed bidParameters test because of extra keys: %s." % str(copyOfBP))
        del copyOfBP

        ABP = client.adgroupBidParameters
        if ABP["STALE_RAISE_IMPRESSION_THRESH"] != 111 or \
                ABP["TAP_THRESHOLD"] != 222 or \
                ABP["LOW_CPI_BID_INCREASE_THRESH"] != 333 or \
                ABP["HIGH_CPI_BID_DECREASE_THRESH"] != 444 or \
                ABP["NO_INSTALL_BID_DECREASE_THRESH"] != 555 or \
                ABP["STALE_RAISE_BID_BOOST"] != 666 or \
                ABP["LOW_CPA_BID_BOOST"] != 777 or \
                ABP["HIGH_CPA_BID_DECREASE"] != 888 or \
                ABP["MAX_BID"] != 999 or \
                ABP["MIN_BID"] != 1111:
            raise ZeroDivisionError("Failed adgroupBidParameters test: %s." % str(ABP))

        copyOfABP = dict(ABP)
        for i in ("STALE_RAISE_IMPRESSION_THRESH",
                  "TAP_THRESHOLD",
                  "LOW_CPI_BID_INCREASE_THRESH",
                  "HIGH_CPI_BID_DECREASE_THRESH",
                  "NO_INSTALL_BID_DECREASE_THRESH",
                  "STALE_RAISE_BID_BOOST",
                  "LOW_CPA_BID_BOOST",
                  "HIGH_CPA_BID_DECREASE",
                  "MAX_BID",
                  "MIN_BID"):
            del copyOfABP[i]

        if len(copyOfABP) != 0:
            raise ZeroDivisionError("Failed adgroupBidParameters test because of extra keys: %s." % str(copyOfABP))
        del copyOfABP

        KAI = client.keywordAdderIds
        if "campaignId" not in KAI or "adGroupId" not in KAI:
            raise ZeroDivisionError("Failed keywordAdderIds test: %s" % str(KAI))

        KAI_CI, KAI_GI = KAI["campaignId"], KAI["adGroupId"]

        if "search" not in KAI_CI or \
                "broad" not in KAI_CI or \
                "exact" not in KAI_CI:
            raise ZeroDivisionError("Failed KAI test: missing s/b/e in campaignId. %s" % str(KAI_CI))

        if "search" not in KAI_GI or \
                "broad" not in KAI_GI or \
                "exact" not in KAI_GI:
            raise ZeroDivisionError("Failed KAI test: missing s/b/e in adGroupId. %s" % str(KAI_GI))

        if KAI_CI["search"] != "searchCampaignId" or \
                KAI_CI["broad"] != "broadCampaignId" or \
                KAI_CI["exact"] != "exactCampaignId":
            raise ZeroDivisionError("Failed KAI test: wrong s/b/e in campaignId. %s" % str(KAI_CI))

        if KAI_GI["search"] != "searchGroupId" or \
                KAI_GI["broad"] != "broadGroupId" or \
                KAI_GI["exact"] != "exactGroupId":
            raise ZeroDivisionError("Failed KAI test: wrong s/b/e in adGroupId. %s" % str(KAI_GI))

        KAP = client.keywordAdderParameters
        if KAP["NEGATIVE_KEYWORD_TAP_THRESHOLD"] != -1001 or \
                KAP["NEGATIVE_KEYWORD_CONVERSION_THRESHOLD"] != -1002 or \
                KAP["TARGETED_KEYWORD_TAP_THRESHOLD"] != -1003 or \
                KAP["TARGETED_KEYWORD_CONVERSION_THRESHOLD"] != -1004 or \
                KAP["EXACT_MATCH_DEFAULT_BID"] != -1005 or \
                KAP["BROAD_MATCH_DEFAULT_BID"] != -1006:
            raise ZeroDivisionError("Failed keywordAdderParameters test: %s." % str(KAP))

        C = client.campaignIds
        if len(C) != 2 or C != (123, 456):
            raise ZeroDivisionError("Failed campaign IDs test: %s." % str(C))

        # Check the storage and retrieval of updatedBidCount. - - - - - - - - - - - -
        updatedBidsPathname = client._getUpdatedBidsCountPathname()
        print("Updated Bids pathname='%s'." % updatedBidsPathname)
        if os.path.exists(updatedBidsPathname):
            os.remove(updatedBidsPathname)

        client.updatedBids = 234
        if client.updatedBids != 234:
            raise ZeroDivisionError("Failed updatedBids test: %s." % client.updatedBids)

        if not os.path.exists(updatedBidsPathname):
            raise ZeroDivisionError("Failed updatedBids file creation test.")

        with open(updatedBidsPathname) as handle:
            data = handle.read()
            if data != "234":
                raise ZeroDivisionError("Failed updatedBids file content test: '%s'." % data)

        os.remove(updatedBidsPathname)

        # Check the storage and retrieval of updatedAdgroupBidCount. - - - - - - - - -
        updatedAdgroupBidsPathname = client._getUpdatedAdgroupBidsCountPathname()
        print("Updated Adgroup Bids pathname='%s'." % updatedAdgroupBidsPathname)
        if os.path.exists(updatedAdgroupBidsPathname):
            os.remove(updatedAdgroupBidsPathname)

        client.updatedAdgroupBids = 12345
        if client.updatedAdgroupBids != 12345:
            raise ZeroDivisionError("Failed updatedAdgroupBids test: %s." % client.updatedAdgroupBids)

        if not os.path.exists(updatedAdgroupBidsPathname):
            raise ZeroDivisionError("Failed updatedAdgroupBids file creation test.")

        with open(updatedAdgroupBidsPathname) as handle:
            data = handle.read()
            if data != "12345":
                raise ZeroDivisionError("Failed updatedAdgroupBids file content test: '%s'." % data)

        os.remove(updatedAdgroupBidsPathname)

        # Check the storage and retrieval of positiveKeywordsAdded. - - - - - - - -
        positiveKeywordsAddedPathname = client._getPositiveKeywordsPathname()
        print("+keywords pathname='%s'." % positiveKeywordsAddedPathname)
        if os.path.exists(positiveKeywordsAddedPathname):
            os.remove(positiveKeywordsAddedPathname)

        client.positiveKeywordsAdded = ['positiveKeyword 1', "positiveKeyword 2"]
        if client.positiveKeywordsAdded != ["positiveKeyword 1", "positiveKeyword 2"]:
            raise ZeroDivisionError("Failed +keywords test: %s." % client.positiveKeywordsAdded)

        if not os.path.exists(positiveKeywordsAddedPathname):
            raise ZeroDivisionError("Failed +keywords file creation test.")

        with open(positiveKeywordsAddedPathname) as handle:
            data = handle.read()
            if data != '["positiveKeyword 1", "positiveKeyword 2"]':
                raise ZeroDivisionError("Failed +keywords file content test: '%s'." % data)

        os.remove(positiveKeywordsAddedPathname)

        # Check the storage and retrieval of negativeKeywordsAdded. - - - - - - - -
        negativeKeywordsAddedPathname = client._getNegativeKeywordsPathname()
        print("-keywords pathname='%s'." % negativeKeywordsAddedPathname)
        if os.path.exists(negativeKeywordsAddedPathname):
            os.remove(negativeKeywordsAddedPathname)

        client.negativeKeywordsAdded = ['negativeKeyword 1', "negativeKeyword 2"]
        if client.negativeKeywordsAdded != ["negativeKeyword 1", "negativeKeyword 2"]:
            raise ZeroDivisionError("Failed -keywords test: %s." % client.negativeKeywordsAdded)

        if not os.path.exists(negativeKeywordsAddedPathname):
            raise ZeroDivisionError("Failed -keywords file creation test.")

        with open(negativeKeywordsAddedPathname) as handle:
            data = handle.read()
            if data != '["negativeKeyword 1", "negativeKeyword 2"]':
                raise ZeroDivisionError("Failed -keywords file content test: '%s'." % data)

        os.remove(negativeKeywordsAddedPathname)

        # Check the history file functionality. - - - - - - - - - - - - - - - - - -
        historyPathname = client._getHistoryPathname()
        print("History pathname='%s'." % historyPathname)
        if os.path.exists(historyPathname):
            os.remove(historyPathname)

        header = "columnA", "columnB", "columnC", "columnD"
        history = "123", "456", "78", "9"
        historyError = "Failed history test"
        client.addRowToHistory(history, header)
        historyContent = client.getHistory()
        if type(historyContent) != list:
            raise ZeroDivisionError("%s: type is %s; s.b. 'list'." % (historyError, type(historyContent)))

        if len(historyContent) != 2:
            raise ZeroDivisionError("%s: len is %s; s.b. 2." % (historyError, len(historyContent)))
        recordedHeader = historyContent[0]
        if len(recordedHeader) != 32:
            raise ZeroDivisionError("%s: header len is %s; s.b. 32; is '%s'." % \
                                    (historyError, len(recordedHeader), recordedHeader))

        if recordedHeader != "%s\n" % ",".join(header):
            raise ZeroDivisionError("%s: header content is %s; s.b. %s." % \
                                    (historyError, recordedHeader, history))

        recordedHistory = historyContent[1]
        if len(recordedHistory) != 13:
            raise ZeroDivisionError("%s: history len is %s; s.b. 13; is '%s'." % \
                                    (historyError, len(recordedHistory), recordedHistory))

        if recordedHistory != "%s\n" % ",".join(history):
            raise ZeroDivisionError("%s: history content is %s; s.b. %s." % \
                                    (historyError, recordedHistory, history))

        totalCostPerInstall = client.getTotalCostPerInstall(1)
        if totalCostPerInstall == None:
            raise ZeroDivisionError("%s: totalCostPerInstall is None; s.b. %f." % \
                                    (historyError, 56.0 / 78))
        if totalCostPerInstall != 56.0 / 78:  # "56" and not "456" because the "4" takes the place of a "$."
            raise ZeroDivisionError("%s: totalCostPerInstall is %40.35f; s.b. %40.35f." % \
                                    (historyError, totalCostPerInstall, 56.0 / 78))

        os.remove(historyPathname)

# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    Client.test()
    # for client in CLIENTS:
    #     print("For client '%s', campaign ids are %s." % (client.clientName, client.campaignIds))
