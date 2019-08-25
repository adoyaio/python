import datetime
from email.headerregistry import Address
import json
import os
#import requests
import sys

from debug import debug, dprint

DATA_DIR                                      = "Data"
CLIENTS_DATA_FILENAME                         = "clients.json"
CLIENT_UPDATED_BIDS_FILENAME_TEMPLATE         = "bids_%s.json"
CLIENT_UPDATED_ADGROUP_BIDS_FILENAME_TEMPLATE = "adgroup_bids_%s.json"
CLIENT_POSITIVE_KEYWORDS_FILENAME_TEMPLATE    = "positive_keywords_%s.json"
CLIENT_NEGATIVE_KEYWORDS_FILENAME_TEMPLATE    = "negative_keywords_%s.json"
CLIENT_HISTORY_FILENAME_TEMPLATE              = "history_%s.csv"
ONE_YEAR_IN_DAYS                              = 365

class Client:
  _LINUX_OS_PLATFORM = "linux"
  _MAC_OS_PLATFORM = "darwin"
#  _GET_REPORTS_URL   = "https://api.searchads.apple.com/api/v1/reports/campaigns" 

  def __init__(self,
               orgId,
               clientName,
               emailAddresses,
               keyFilename, pemFilename,
               bidParameters,
               adgroupBidParameters,
               campaignIds,
               keywordAdderIds,
               keywordAdderParameters
               ):
    if "campaignId" not in keywordAdderIds or \
       "adGroupId"  not in keywordAdderIds:
      raise NameError("Missing campaignId or adGroupId in keywordAdderIds")

    kAPCI, kAPGI = keywordAdderIds["campaignId"], keywordAdderIds["adGroupId"]

    if "search" not in kAPCI or "broad" not in kAPCI or "exact" not in kAPCI:
      raise NameError("Missing search, broad, or exact in keywordAdderIds[\"campaignId\"]. It was %s." % str(kAPCI))

    if "search" not in kAPGI or "broad" not in kAPGI or "exact" not in kAPGI:
      raise NameError("Missing search, broad, or exact in keywordAdderIds[\"adGroupId\"]. It was %s." % str(kAPGI))

    self._orgId                   = orgId
    self._clientName              = clientName
    self._emailAddresses          = emailAddresses
    self._keyFilename             = keyFilename
    self._pemFilename             = pemFilename
    self._bidParameters           = bidParameters
    self._adgroupBidParameters    = adgroupBidParameters
    self._campaignIds             = campaignIds
    self._keywordAdderIds         = keywordAdderIds
    self._keywordAdderParameters  = keywordAdderParameters
    self._updatedBidsCount        = self._readUpdatedBidsCount()
    self._updatedAdgroupBidsCount = self._readUpdatedAdgroupBidsCount()
    self._positiveKeywordsAdded   = self._readPositiveKeywordsAdded()
    self._negativeKeywordsAdded   = self._readNegativeKeywordsAdded()

    # The history data is populated when requested.


  def __str__(self) : return "Client '%s (#%d)" % (self.clientName, self.orgId)

  @property
  def orgId(self)                  : return self._orgId
  @property
  def clientName(self)             : return self._clientName
  @property
  def emailAddresses(self)         : return list(self._emailAddresses)
  @property
  def keyPathname(self)            : return os.path.join(self._getCertificatesPath(), self._keyFilename)
  @property
  def pemPathname(self)            : return os.path.join(self._getCertificatesPath(), self._pemFilename)
  @property
  def bidParameters(self)          : return dict(self._bidParameters)
  @property
  def adgroupBidParameters(self)   : return dict(self._adgroupBidParameters)
  @property
  def keywordAdderIds(self)        : return dict(self._keywordAdderIds) # TODO: Deep copy. --DS, 14-Sep-2018
  @property
  def keywordAdderParameters(self) : return dict(self._keywordAdderParameters)
  @property
  def campaignIds(self)            : return tuple(self._campaignIds)
  @property
  def updatedBids(self)            : return self._updatedBidsCount

  @updatedBids.setter
  def updatedBids(self, newValue):
    if self._updatedBidsIsStale == False:
      self._updatedBidsCount = newValue
      self._updatedBidsIsStale = True

    else:
      self._updatedBidsCount += newValue

    self._writeStateInformation(self._getUpdatedBidsCountPathname(), self._updatedBidsCount)

  @property
  def updatedAdgroupBids(self)     : return self._updatedAdgroupBidsCount

  @updatedAdgroupBids.setter
  def updatedAdgroupBids(self, newValue):
    if self._updatedAdgroupBidsIsStale == False:
      self._updatedAdgroupBidsCount = newValue
      self._updatedAdgroupBidsIsStale = True

    else:
      self._updatedAdgroupBidsCount += newValue

    self._writeStateInformation(self._getUpdatedAdgroupBidsCountPathname(), self._updatedAdgroupBidsCount)

  @property
  def positiveKeywordsAdded(self)  : return self._positiveKeywordsAdded
  @property
  def negativeKeywordsAdded(self)  : return self._negativeKeywordsAdded

  @positiveKeywordsAdded.setter
  def positiveKeywordsAdded(self, newValue):
    self._positiveKeywordsAdded = newValue
    self._writeStateInformation(self._getPositiveKeywordsPathname(), self._positiveKeywordsAdded)
    

  @negativeKeywordsAdded.setter
  def negativeKeywordsAdded(self, newValue):
    self._negativeKeywordsAdded = newValue
    self._writeStateInformation(self._getNegativeKeywordsPathname(), self._negativeKeywordsAdded)
    

  #@property def campaignData(self)  : return self._campaignData
  #def setCampaignData(self, campaignData) : self._campaignData = campaignData


  def _getUpdatedBidsCountPathname(self):
    return os.path.join(DATA_DIR, CLIENT_UPDATED_BIDS_FILENAME_TEMPLATE % self.orgId)
    

  def _getUpdatedAdgroupBidsCountPathname(self):
    return os.path.join(DATA_DIR, CLIENT_UPDATED_ADGROUP_BIDS_FILENAME_TEMPLATE % self.orgId)
    

  def _getPositiveKeywordsPathname(self):
    return os.path.join(DATA_DIR, CLIENT_POSITIVE_KEYWORDS_FILENAME_TEMPLATE % self.orgId)
    

  def _getNegativeKeywordsPathname(self):
    return os.path.join(DATA_DIR, CLIENT_NEGATIVE_KEYWORDS_FILENAME_TEMPLATE % self.orgId)
    

  def _writeStateInformation(self, pathname, newValue):
    # TODO: Convert this to store/retrieve the data to a database, not files. --DS, 28-Aug-2018
    with open(pathname, "w") as handle:
      json.dump(newValue, handle)
    

  def _readStateInformation(self, pathname, defaultValue):
    if os.path.exists(pathname):
      with open(pathname) as handle:
        result = json.load(handle)

    else:
      result = defaultValue

    return result


  def _readUpdatedBidsCount(self):
    self._updatedBidsIsStale = False
    return self._readStateInformation(self._getUpdatedBidsCountPathname(), 0)


  def _readUpdatedAdgroupBidsCount(self):
    self._updatedAdgroupBidsIsStale = False
    return self._readStateInformation(self._getUpdatedAdgroupBidsCountPathname(), 0)


  def _readPositiveKeywordsAdded(self):
    return self._readStateInformation(self._getPositiveKeywordsPathname(), [])


  def _readNegativeKeywordsAdded(self):
    return self._readStateInformation(self._getNegativeKeywordsPathname(), [])



#^  # ----------------------------------------------------------------------------
#^  @property
#^  @debug
#^  def campaignIds(self):
#^    today = datetime.date.today()
#^
#^    payload = {
#^                "startTime"                  : str(today), 
#^                "endTime"                    : str(today),
#^                "returnRowTotals"            : True, 
#^                "returnRecordsWithNoMetrics" : True,
#^                "selector" : {
#^                  "orderBy"    : [ { "field" : "localSpend", "sortOrder" : "DESCENDING" } ], 
#^                  "fields"     : [ "localSpend", "taps", "impressions", "conversions", "avgCPA", "avgCPT", "ttr", "conversionRate" ],
#^                  "pagination" : { "offset" : 0, "limit" : 1000 }
#^                }, 
#^                #"groupBy"                    : [ "COUNTRY_CODE" ], 
#^                #"granularity"                : 2, # 1 is hourly, 2 is daily, 3 is monthly etc
#^              }
#^  
#^    headers = { "Authorization": "orgId=%s" % self.orgId } 
#^
#^    dprint("Headers: %s\n" % headers)
#^    dprint("Payload: %s\n" % payload)
#^    #dprint("Apple 'Get Reports' URL: %s\n" % GET_REPORTS_URL)
#^  
#^    response = requests.post(Client._GET_REPORTS_URL,
#^                             cert=(self.pemPathname, self.keyPathname),
#^                             json=payload,
#^                             headers=headers)
#^  
#^    dprint("Response: '%s'" % response)
#^    
#^    return [ item["metadata"]["campaignId"] for item in json.loads(response.text)["data"]["reportingDataResponse"]["row"] ]



  # ----------------------------------------------------------------------------
  def _getCertificatesPath(self) :
    return '/home/scott/ScottKaplan/Certificates' if sys.platform == Client._LINUX_OS_PLATFORM or sys.platform == Client._MAC_OS_PLATFORM else \
             'C:/Users/A/Desktop/apple_search_ads_api'



  # ----------------------------------------------------------------------------
  def _getHistoryPathname(self):
    return os.path.join(DATA_DIR, CLIENT_HISTORY_FILENAME_TEMPLATE % self.orgId)



  # ----------------------------------------------------------------------------
  def addRowToHistory(self, stuff, headerStuff):
    pathname = self._getHistoryPathname()

    if not os.path.exists(pathname):
      with open(pathname, "w") as handle:
        handle.write("%s\n" % ",".join(headerStuff))

    with open(pathname, "a") as handle:
      handle.write("%s\n" % ",".join(stuff))



  # ----------------------------------------------------------------------------
  def getHistory(self):
    pathname = self._getHistoryPathname()

    # A call to "addRowToHistory()" is always made before reading the history so
    # the file should exist. Therefore, we don't check that the file exists
    # here; if it doesn't, we want the program to fail.

    with open(pathname, "r") as handle:
      # TODO: [Performance] Yes, I know this won't scale. It's ok for now, however. --DS, 13-Jan-2019
      return handle.readlines()[-ONE_YEAR_IN_DAYS:]



  # ----------------------------------------------------------------------------
  def getTotalCostPerInstall(self, daysToLookBack):
    if not os.path.exists(self._getHistoryPathname()):
      return None # EARLY RETURN

    lines = self.getHistory()

    if len(lines) <= daysToLookBack: # <= rather than < to account for the header row.
      return None # EARLY RETURN

    totalCost, totalInstalls = 0.0, 0
    for line in lines[-daysToLookBack:]:
      tokens = line.rstrip().split(",")
      totalCost     += float(tokens[1][1:])
      totalInstalls += int(tokens[2])
      
    return totalCost / totalInstalls



  # ----------------------------------------------------------------------------
  @staticmethod
  def test():
    client = Client("Org_123",
                    "Clientname_123", 
                    ("Email_Address_1", "Email_Address_2"),
                    "Key_filename_123",
                    "Pem_filename_123",
                    {
                     "STALE_RAISE_IMPRESSION_THRESH"  : 11,
                     "TAP_THRESHOLD"                  : 22,
                     "HIGH_CPI_BID_DECREASE_THRESH"   : 44,
                     "NO_INSTALL_BID_DECREASE_THRESH" : 55,
                     "STALE_RAISE_BID_BOOST"          : 66,
                     "LOW_CPA_BID_BOOST"              : 77,
                     "HIGH_CPA_BID_DECREASE"          : 88,
                     "MAX_BID"                        : 99,
                     "MIN_BID"                        : 111,
                    },
                    {
                     "STALE_RAISE_IMPRESSION_THRESH"  : 111,
                     "TAP_THRESHOLD"                  : 222,
                     "LOW_CPI_BID_INCREASE_THRESH"    : 333,
                     "HIGH_CPI_BID_DECREASE_THRESH"   : 444,
                     "NO_INSTALL_BID_DECREASE_THRESH" : 555,
                     "STALE_RAISE_BID_BOOST"          : 666,
                     "LOW_CPA_BID_BOOST"              : 777,
                     "HIGH_CPA_BID_DECREASE"          : 888,
                     "MAX_BID"                        : 999,
                     "MIN_BID"                        : 1111,
                    },
                    (123, 456),
                    {"campaignId" : { "search" : "searchCampaignId",
                                      "broad"  : "broadCampaignId",
                                      "exact"  : "exactCampaignId",
                                    },
                     "adGroupId"  : { "search" : "searchGroupId",
                                      "broad"  : "broadGroupId",
                                      "exact"  : "exactGroupId",
                                    },
                    },
                    {
                      "NEGATIVE_KEYWORD_TAP_THRESHOLD"        : -1001,
                      "NEGATIVE_KEYWORD_CONVERSION_THRESHOLD" : -1002 ,
                      "TARGETED_KEYWORD_TAP_THRESHOLD"        : -1003 ,
                      "TARGETED_KEYWORD_CONVERSION_THRESHOLD" : -1004 ,
                      "EXACT_MATCH_DEFAULT_BID"               : -1005 ,
                      "BROAD_MATCH_DEFAULT_BID"               : -1006,
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
    if BP["STALE_RAISE_IMPRESSION_THRESH"]  != 11 or \
       BP["TAP_THRESHOLD"]                  != 22 or \
       BP["HIGH_CPI_BID_DECREASE_THRESH"]   != 44 or \
       BP["NO_INSTALL_BID_DECREASE_THRESH"] != 55 or \
       BP["STALE_RAISE_BID_BOOST"]          != 66 or \
       BP["LOW_CPA_BID_BOOST"]              != 77 or \
       BP["HIGH_CPA_BID_DECREASE"]          != 88 or \
       BP["MAX_BID"]                        != 99 or \
       BP["MIN_BID"]                        != 111:
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
    if ABP["STALE_RAISE_IMPRESSION_THRESH"]  != 111 or \
       ABP["TAP_THRESHOLD"]                  != 222 or \
       ABP["LOW_CPI_BID_INCREASE_THRESH"]    != 333 or \
       ABP["HIGH_CPI_BID_DECREASE_THRESH"]   != 444 or \
       ABP["NO_INSTALL_BID_DECREASE_THRESH"] != 555 or \
       ABP["STALE_RAISE_BID_BOOST"]          != 666 or \
       ABP["LOW_CPA_BID_BOOST"]              != 777 or \
       ABP["HIGH_CPA_BID_DECREASE"]          != 888 or \
       ABP["MAX_BID"]                        != 999 or \
       ABP["MIN_BID"]                        != 1111:
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
       "broad" not in KAI_CI  or \
       "exact" not in KAI_CI:
      raise ZeroDivisionError("Failed KAI test: missing s/b/e in campaignId. %s" % str(KAI_CI))

    if "search" not in KAI_GI or \
       "broad" not in KAI_GI  or \
       "exact" not in KAI_GI:
      raise ZeroDivisionError("Failed KAI test: missing s/b/e in adGroupId. %s" % str(KAI_GI))

    if KAI_CI["search"] != "searchCampaignId" or \
       KAI_CI["broad"]  != "broadCampaignId"  or \
       KAI_CI["exact"]  != "exactCampaignId":
      raise ZeroDivisionError("Failed KAI test: wrong s/b/e in campaignId. %s" % str(KAI_CI))

    if KAI_GI["search"] != "searchGroupId" or \
       KAI_GI["broad"]  != "broadGroupId"  or \
       KAI_GI["exact"]  != "exactGroupId":
      raise ZeroDivisionError("Failed KAI test: wrong s/b/e in adGroupId. %s" % str(KAI_GI))

    KAP = client.keywordAdderParameters
    if KAP["NEGATIVE_KEYWORD_TAP_THRESHOLD"]        != -1001 or \
       KAP["NEGATIVE_KEYWORD_CONVERSION_THRESHOLD"] != -1002 or \
       KAP["TARGETED_KEYWORD_TAP_THRESHOLD"]        != -1003 or \
       KAP["TARGETED_KEYWORD_CONVERSION_THRESHOLD"] != -1004 or \
       KAP["EXACT_MATCH_DEFAULT_BID"]               != -1005 or \
       KAP["BROAD_MATCH_DEFAULT_BID"]               != -1006 :
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

    header  = "columnA", "columnB", "columnC", "columnD"
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
    if totalCostPerInstall != 56.0 / 78: # "56" and not "456" because the "4" takes the place of a "$."
      raise ZeroDivisionError("%s: totalCostPerInstall is %40.35f; s.b. %40.35f." % \
                              (historyError, totalCostPerInstall, 56.0 / 78))
  
    os.remove(historyPathname)



# ------------------------------------------------------------------------------

CLIENTS = [ Client(client["orgId"],
                   client["clientName"],
                   [ Address(emailAddress["name"],
                             emailAddress["emailName"],
                             emailAddress["domain"]) for emailAddress in client["emailAddresses"] ],
                   client["keyFilename"],
                   client["pemFilename"],
                   client["bidParameters"],
                   client["adgroupBidParameters"],
                   client["campaignIds"],
                   client["keywordAdderIds"],
                   client["keywordAdderParameters"],
                  )
              for client in json.load(open(os.path.join(DATA_DIR, CLIENTS_DATA_FILENAME))) \
                if client.get("disabled", False) == False]



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  Client.test()

  for client in CLIENTS:
    print("For client '%s', campaign ids are %s." % (client.clientName, client.campaignIds))
