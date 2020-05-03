from __future__ import print_function  # Python 2/3 compatibility
import logging
import decimal
import boto3
from collections import defaultdict
import datetime
import json
import pandas as pd
import pprint
import requests
import sys
import time
from utils import DynamoUtils

from botocore.exceptions import ClientError

from Client import CLIENTS
from configuration import EMAIL_FROM, \
    APPLE_UPDATE_POSITIVE_KEYWORDS_URL, \
    APPLE_KEYWORD_REPORTING_URL_TEMPLATE, \
    TOTAL_COST_PER_INSTALL_LOOKBACK, \
    HTTP_REQUEST_TIMEOUT, \
    BRANCH_ANALYTICS_URL_BASE, \
    data_sources, \
    aggregations
from debug import debug, dprint

sendG = False  # Set to True to enable sending data to Apple, else a test run.
logger = logging.getLogger()
logger.setLevel(logging.INFO)

###### date and time parameters for bidding lookback ######
BIDDING_LOOKBACK = 7  # days #make this 2
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(BIDDING_LOOKBACK)
start_date = today - start_date_delta
end_date = today - end_date_delta



# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


# ------------------------------------------------------------------------------
@debug
def initialize(env, dynamoEndpoint):
    global sendG
    global dynamodb

    if env != "prod":
        sendG = False
        #dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1', endpoint_url=dynamoEndpoint)
    else:
        sendG = True
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    logger.info("In runBranchBidAdjuster:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


# ------------------------------------------------------------------------------
@debug
def process():




    for client in CLIENTS:
        print("Apple and Branch keyword data from : " + str(client.clientName))
        print(client.orgId)
        adgroup_keys = client.keywordAdderIds["adGroupId"].keys()

        for adgroup_key in adgroup_keys:
            for adgroup_id in [client.keywordAdderIds["adGroupId"][adgroup_key]]:  # iterate all adgroups
                print("pulling adgroup_id " + str(adgroup_id))

                #get apple data
                kw_response = DynamoUtils.getAppleKeywordData(dynamodb, adgroup_id, start_date, end_date)
                print("querying with :::" + str(start_date))
                print("querying with :::" + str(end_date))
                print("got back:::" + str(kw_response["Count"]))

                #print("kw_response::::" + str(kw_response))
                #initialize the dict which will end up as our dataframe
                keyword_info = defaultdict(list)

                for kw_data in kw_response[u'Items']:
                    #print(json.dumps(kw_data, cls=DecimalEncoder))
                    keyword = kw_data['keyword']
                    date = kw_data['date']
                    branch_revenue = 0
                    branch_commerce_event_count = 0

                    #get branch data
                    print("check branch data for " + keyword + " " + date)
                    branch_response = DynamoUtils.getBranchCommerceEvents(dynamodb, adgroup_id, keyword, date)
                    for j in branch_response[u'Items']:
                        print("found branch result:::")
                        print(json.dumps(j, cls=DecimalEncoder))
                        if len(branch_response['Items']) > 0:
                            branch_revenue = branch_response['Items'][0]["revenue"]
                            branch_commerce_event_count = branch_response['Items'][0]["count"]

                    # initialize data frame
                    keyword_info["keyword"].append(kw_data["keyword"])
                    keyword_info["keywordId"].append(kw_data["keyword_id"])
                    keyword_info["keywordStatus"].append(kw_data["keywordStatus"])
                    keyword_info["matchType"].append(kw_data["matchType"])
                    keyword_info["adGroupName"].append(kw_data["adgroup_name"])
                    keyword_info["adGroupId"].append(kw_data["adgroup_id"])
                    keyword_info["adGroupDeleted"].append(kw_data["adgroup_deleted"])
                    keyword_info["bid"].append(kw_data["bid"])
                    keyword_info["deleted"].append(kw_data["deleted"])
                    # TODO not seeing this in dynamo
                    #  keyword_info["keywordDisplayStatus"].append(kw_data["keywordDisplayStatus"])
                    keyword_info["modificationTime"].append(kw_data["modification_time"])
                    keyword_info["date"].append(kw_data["date"])
                    keyword_info["impressions"].append(kw_data["impressions"])
                    keyword_info["taps"].append(kw_data["taps"])
                    keyword_info["ttr"].append(kw_data["ttr"])
                    keyword_info["installs"].append(kw_data["installs"])
                    keyword_info["newDownloads"].append(kw_data["new_downloads"])
                    keyword_info["redownloads"].append(kw_data["re_downloads"])
                    keyword_info["latOnInstalls"].append(kw_data["lat_on_installs"])
                    keyword_info["latOffInstalls"].append(kw_data["lat_off_installs"])
                    keyword_info["avgCPA"].append(kw_data["avg_cpa"])
                    keyword_info["conversionRate"].append(kw_data["conversion_rate"])
                    keyword_info["localSpend"].append(kw_data["local_spend"])
                    keyword_info["avgCPT"].append(kw_data["avg_cpt"])

                    # BRANCH fields
                    keyword_info["branch_commerce_event_count"].append(branch_commerce_event_count)
                    keyword_info["branch_revenue"].append(branch_revenue)

                    #df_keyword_info = pd.DataFrame(keyword_info)
                    #dprint("df_keyword_info=%s." % str(df_keyword_info))

                    #dprint("keyword_info=%s." % pprint.pformat(keyword_info))
                    export_dict_to_csv(keyword_info, str(adgroup_id) + 'keyword_info.csv')


def export_dict_to_csv(raw_dict, filename):
  '''
  This function takes a json and a filename, and it exports the json as a csv to the given filename.
  '''
  df = pd.DataFrame.from_dict(raw_dict)
  df.to_csv(filename, index = None)

# ------------------------------------------------------------------------------
@debug
def terminate():
    pass


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    initialize('lcl', 'http://localhost:8000')
    process()
    terminate()

    # initialize('prod', 'http://localhost:8000')
    # process()
    # terminate()