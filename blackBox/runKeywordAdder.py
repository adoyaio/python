import logging
from collections import defaultdict
import datetime
from datetime import datetime as dt
import json
import pandas as pd
import pprint
import re
import requests
import time
from utils import EmailUtils, DynamoUtils, S3Utils, LambdaUtils
import boto3
from utils.debug import debug, dprint
from utils.retry import retry
from Client import Client
from configuration import config

JSON_MIME_TYPES  = ("application/json", "text/json")
DUPLICATE_KEYWORD_REGEX = re.compile("(NegativeKeywordImport|KeywordImport)\[(?P<index>\d+)\]\.text")

date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(days=365)
start_date = today - start_date_delta
end_date = today - end_date_delta

# FOR QA PURPOSES set these fields explicitly
#start_date = dt.strptime('2019-12-15', '%Y-%m-%d').date()
#end_date = dt.strptime('2019-12-22', '%Y-%m-%d').date()

@debug
def initialize(env, dynamoEndpoint, emailToInternal):
    global sendG
    global clientsG
    global dynamodb
    global EMAIL_TO
    global logger
    
    EMAIL_TO = emailToInternal
    sendG = LambdaUtils.getSendG(env)
    dynamodb = LambdaUtils.getDynamoHost(env,dynamoEndpoint)
    clientsG = Client.getClients(dynamodb)

    logger = LambdaUtils.getLogger(env)
    logger.info("In runKeywordAdder:::initialize(), sendG='%s', dynamoEndpoint='%s'" % (sendG, dynamoEndpoint))


@retry
def getSearchTermsReportFromAppleHelper(url, cert, json, headers):
  return requests.post(
    url, 
    cert=cert, 
    json=json, 
    headers=headers, 
    timeout=config.HTTP_REQUEST_TIMEOUT
  )


def getSearchTermsReportFromApple(client, campaignId):
  payload = {
    "startTime": str(start_date), 
    "endTime": str(end_date),
    #"granularity": 2, # 1=hourly, 2=daily, 3=monthly, etc.
    "selector": {
      "orderBy": [
        {
          "field": "localSpend",
          "sortOrder": "DESCENDING"
        }
      ], 
      "fields": [
        "localSpend",
        "taps",
        "impressions",
        "installs",
        "avgCPA",
        "avgCPT",
        "ttr",
        "conversionRate"
      ],
      "pagination": { 
        "offset": 0,
        "limit": 1000
      }
    },
    #"groupBy": ["COUNTRY_CODE"], 
    "returnRowTotals": True, 
    "returnRecordsWithNoMetrics": False
  }
  url = config.APPLE_KEYWORD_SEARCH_TERMS_URL_TEMPLATE % campaignId
  headers = { "Authorization": "orgId=%s" % client.orgId }
  dprint ("URL is '%s'." % url)
  dprint ("Payload is '%s'." % payload)
  dprint ("Headers are %s." % headers)
  response = getSearchTermsReportFromAppleHelper(
    url,
    cert=(S3Utils.getCert(client.pemFilename), S3Utils.getCert(client.keyFilename)),
    json=payload,
    headers=headers
  )
  if response.status_code != 200:
        email = "client id:%d \n url:%s \n response:%s" % (client.orgId, url, response)
        date = time.strftime("%m/%d/%Y")
        subject ="%s - %d ERROR in runKeywordAdder for %s" % (date, response.status_code, client.clientName)
        logger.warn(email)
        logger.error(subject)
        if sendG:
            EmailUtils.sendTextEmail(email, subject, EMAIL_TO, [], config.EMAIL_FROM)

  dprint ("Response is %s." % response)
  return json.loads(response.text) 

def analyzeKeywordsSharedCode(
  KAP,
  targeted_kws_pre_de_dupe_text_only_second_step,
  negative_kws_pre_de_dupe_text_only_second_step,
  search_match_campaign_id,
  broad_match_campaign_id,
  exact_match_campaign_id,
  search_match_ad_group_id,
  broad_match_ad_group_id,
  exact_match_ad_group_id,
  currency
):
  #deploy negative keywords accross search and broad match campaigns by first creating a dataframe
  #combine negative and targeted keywords as you have to negative exact match all of them
  all_negatives_combined_first_step_df = [targeted_kws_pre_de_dupe_text_only_second_step, negative_kws_pre_de_dupe_text_only_second_step]
  all_negatives_combined_second_step_df = pd.concat(all_negatives_combined_first_step_df)
  all_negatives_combined_third_step_df = pd.DataFrame(all_negatives_combined_second_step_df)
  all_negatives_combined_fourth_step_df = all_negatives_combined_third_step_df.rename(columns={'searchTermText': 'text'})
  
  #rename for search match negative matching
  search_match_negatives_df = all_negatives_combined_fourth_step_df.copy()
  
  #rename for broad match negative matching
  broad_match_negatives_df = all_negatives_combined_fourth_step_df.copy()
  
  #create dataframe for search match negatives
  #add action type column and update value as per apple search api requirement

  # JF 03/23/2020 apple v2 removing this field for v2
  #search_match_negatives_df['importAction'] = search_match_negatives_df.shape[0]*['CREATE']

  # JF 03/23/2020 apple v2 updgrade removing campaign and ad group ids from payload
  #add campaign id column and update value as per apple search api requirement
  #search_match_negatives_df['campaignId'] = search_match_negatives_df.shape[0]*[search_match_campaign_id]
  
  #add ad group id column and update value as per apple search api requirement
  #search_match_negatives_df['adGroupId'] = search_match_negatives_df.shape[0]*[search_match_ad_group_id]

  #add match type column and update value as per apple search api requirement
  search_match_negatives_df['matchType'] = search_match_negatives_df.shape[0]*['EXACT']
  
  #add status column and update value as per apple search api requirement
  search_match_negatives_df['status'] = search_match_negatives_df.shape[0]*['ACTIVE']
  
  #create dataframe for broad match negatives
  #add action type column and update value as per apple broad api requirement

  # JF 03/23/2020 apple v2 removing this field for v2
  #broad_match_negatives_df['importAction'] = broad_match_negatives_df.shape[0]*['CREATE']

  # JF 03/23/2020 apple v2 updgrade removing campaign and ad group ids from payload
  #add campaign id column and update value as per apple broad api requirement
  #broad_match_negatives_df['campaignId'] = broad_match_negatives_df.shape[0]*[broad_match_campaign_id]
  
  #add ad group id column and update value as per apple broad api requirement
  #broad_match_negatives_df['adGroupId'] = broad_match_negatives_df.shape[0]*[broad_match_ad_group_id]
  
  #add match type column and update value as per apple broad api requirement
  broad_match_negatives_df['matchType'] = broad_match_negatives_df.shape[0]*['EXACT']
  
  #add status column and update value as per apple search api requirement
  broad_match_negatives_df['status'] = broad_match_negatives_df.shape[0]*['ACTIVE']
  
  #convert search and broad match negative dataframes into jsons for uploading
  search_match_negatives_for_upload = search_match_negatives_df.to_json(orient = 'records')
  broad_match_negatives_for_upload = broad_match_negatives_df.to_json(orient = 'records')

  #JF 03/23/2020 apple v2 updgrade adding url for search match and broad match negatives
  search_match_negatives_url = config.APPLE_UPDATE_NEGATIVE_KEYWORDS_URL % (search_match_campaign_id, search_match_ad_group_id)
  broad_match_negatives_url = config.APPLE_UPDATE_NEGATIVE_KEYWORDS_URL % (broad_match_campaign_id, broad_match_ad_group_id)

  #create dataframe for targeted keywords
  #update column name for targeted keywords & convert into dataframe
  targeted_kws_pre_de_dupe_text_only_third_step_df = pd.DataFrame(targeted_kws_pre_de_dupe_text_only_second_step)
  targeted_kws_pre_de_dupe_text_only_fourth_step_df = targeted_kws_pre_de_dupe_text_only_third_step_df.rename(columns={'searchTermText': 'text'})
  
  #create separate variables for targeted exact and broad match additions
  exact_match_targeted_first_step_df = targeted_kws_pre_de_dupe_text_only_fourth_step_df.copy()
  broad_match_targeted_first_step_df = targeted_kws_pre_de_dupe_text_only_fourth_step_df.copy()
  
  #create exact match keyword file for uploading
  #add action type column and update value as per apple broad api requirement

  # JF 03/23/2020 apple v2 removing this field for v2
  #exact_match_targeted_first_step_df['importAction'] = exact_match_targeted_first_step_df.shape[0]*['CREATE']
  
  #add match type column and update value as per apple broad api requirement
  exact_match_targeted_first_step_df['matchType'] = exact_match_targeted_first_step_df.shape[0]*['EXACT']
  
  #add match type column and update value as per apple broad api requirement
  exact_match_targeted_first_step_df['status'] = exact_match_targeted_first_step_df.shape[0]*['ACTIVE']

  # JF 03/23/2020 apple v2 updgrade removing campaign and ad group ids from payload
  #add campaign id column and update value as per apple search api requirement
  #exact_match_targeted_first_step_df['campaignId'] = exact_match_targeted_first_step_df.shape[0]*[exact_match_campaign_id]
  
  #add ad group id column and update value as per apple search api requirement
  #exact_match_targeted_first_step_df['adGroupId'] = exact_match_targeted_first_step_df.shape[0]*[exact_match_ad_group_id]
  
  #add bid column and update value as per apple search api requirement
  exact_match_targeted_first_step_df['bidAmount'] = exact_match_targeted_first_step_df.shape[0]*[KAP["EXACT_MATCH_DEFAULT_BID"]]
  
  #add bid column and update value as per apple search api requirement
  exact_match_targeted_first_step_df['bidAmount'] = exact_match_targeted_first_step_df.shape[0]*[{"amount":""+str(KAP["EXACT_MATCH_DEFAULT_BID"]), "currency":currency}]
  
  #create broad match keyword file for uploading
  #add action type column and update value as per apple broad api requirement

  # JF 03/23/2020 apple v2 removing this field for v2
  #broad_match_targeted_first_step_df['importAction'] = broad_match_targeted_first_step_df.shape[0]*['CREATE']
  
  #add match type column and update value as per apple broad api requirement
  broad_match_targeted_first_step_df['matchType'] = broad_match_targeted_first_step_df.shape[0]*['BROAD']
  
  #add match type column and update value as per apple broad api requirement
  broad_match_targeted_first_step_df['status'] = broad_match_targeted_first_step_df.shape[0]*['ACTIVE']

  # JF 03/23/2020 apple v2 updgrade removing campaign and ad group ids from payload
  #add campaign id column and update value as per apple search api requirement
  #broad_match_targeted_first_step_df['campaignId'] = broad_match_targeted_first_step_df.shape[0]*[broad_match_campaign_id]
  
  #add ad group id column and update value as per apple search api requirement
  #broad_match_targeted_first_step_df['adGroupId'] = broad_match_targeted_first_step_df.shape[0]*[broad_match_ad_group_id]
  
  #add bid column and update value as per apple search api requirement
  broad_match_targeted_first_step_df['bidAmount'] = broad_match_targeted_first_step_df.shape[0]*[KAP["BROAD_MATCH_DEFAULT_BID"]]
  
  #add bid column and update value as per apple search api requirement
  broad_match_targeted_first_step_df['bidAmount'] = broad_match_targeted_first_step_df.shape[0]*[{"amount":""+str(KAP["BROAD_MATCH_DEFAULT_BID"]), "currency":currency}]

  exact_match_targeted_url = config.APPLE_UPDATE_POSITIVE_KEYWORDS_URL % (exact_match_campaign_id, exact_match_ad_group_id)
  broad_match_targeted_url = config.APPLE_UPDATE_POSITIVE_KEYWORDS_URL % (broad_match_campaign_id, broad_match_ad_group_id)

  #convert search and broad match targeted dataframes into jsons for uploading
  exact_match_targeted_for_upload = exact_match_targeted_first_step_df.to_json(orient = 'records')
  broad_match_targeted_for_upload = broad_match_targeted_first_step_df.to_json(orient = 'records')

  return exact_match_targeted_for_upload, \
         exact_match_targeted_url, \
         broad_match_targeted_for_upload, \
         broad_match_targeted_url, \
         search_match_negatives_for_upload, \
         search_match_negatives_url, \
         broad_match_negatives_for_upload, \
         broad_match_negatives_url


def analyzeKeywords(
  search_match_data, 
  broad_match_data, 
  ids, 
  keywordAdderParameters, 
  currency
):
  KAP = keywordAdderParameters;
  
  #nested dictionary containing search term data
  search_match_extract_first_step = search_match_data["data"]["reportingDataResponse"]
  
  #second part of dictionary extraction
  search_match_extract_second_step = search_match_extract_first_step['row']
  
  #compile data from json library and put into dataframe
  search_match_extract_third_step = defaultdict(list)
  
  for r in search_match_extract_second_step:
      search_match_extract_third_step['searchTermText'].append(r['metadata']['searchTermText'])
      search_match_extract_third_step['impressions'].append(r['total']['impressions'])
      search_match_extract_third_step['taps'].append(r['total']['taps'])
      search_match_extract_third_step['ttr'].append(r['total']['ttr'])
      search_match_extract_third_step['installs'].append(r['total']['installs'])
      search_match_extract_third_step['newDownloads'].append(r['total']['newDownloads'])
      search_match_extract_third_step['redownloads'].append(r['total']['redownloads'])
      search_match_extract_third_step['latOnInstalls'].append(r['total']['latOnInstalls'])
      search_match_extract_third_step['latOffInstalls'].append(r['total']['latOffInstalls'])
      search_match_extract_third_step['avgCPA'].append(r['total']['avgCPA']['amount'])
      search_match_extract_third_step['conversionRate'].append(r['total']['conversionRate'])
      search_match_extract_third_step['localSpend'].append(r['total']['localSpend']['amount'])	
      search_match_extract_third_step['avgCPT'].append(r['total']['avgCPT']['amount'])
  
  #convert to dataframe    
  search_match_extract_df = pd.DataFrame(search_match_extract_third_step)
  
  ####### broad match broad queries#######
  
  #nested dictionary containing broad term data
  broad_match_extract_first_step = broad_match_data["data"]["reportingDataResponse"]
  
  #second part of dictionary extraction
  broad_match_extract_second_step = broad_match_extract_first_step['row']
  
  #compile data from json library and put into dataframe
  broad_match_extract_third_step = defaultdict(list)
  
  for r in broad_match_extract_second_step:
    broad_match_extract_third_step['searchTermText'].append(r['metadata']['searchTermText'])
    broad_match_extract_third_step['impressions'].append(r['total']['impressions'])
    broad_match_extract_third_step['taps'].append(r['total']['taps'])
    broad_match_extract_third_step['ttr'].append(r['total']['ttr'])
    broad_match_extract_third_step['installs'].append(r['total']['installs'])
    broad_match_extract_third_step['newDownloads'].append(r['total']['newDownloads'])
    broad_match_extract_third_step['redownloads'].append(r['total']['redownloads'])
    broad_match_extract_third_step['latOnInstalls'].append(r['total']['latOnInstalls'])
    broad_match_extract_third_step['latOffInstalls'].append(r['total']['latOffInstalls'])
    broad_match_extract_third_step['avgCPA'].append(r['total']['avgCPA']['amount'])
    broad_match_extract_third_step['conversionRate'].append(r['total']['conversionRate'])
    broad_match_extract_third_step['localSpend'].append(r['total']['localSpend']['amount'])	
    broad_match_extract_third_step['avgCPT'].append(r['total']['avgCPT']['amount'])
  
  #convert to dataframe    
  broad_match_extract_df = pd.DataFrame(broad_match_extract_third_step)
  
  #combine each data frame into one
  all_match_type_combine_first_step_df = [search_match_extract_df, broad_match_extract_df]
  all_match_type_combine_second_step_df = pd.concat(all_match_type_combine_first_step_df)
  
  #aggregate search query data
  all_search_queries = all_match_type_combine_second_step_df.groupby('searchTermText')['installs','taps'].sum().reset_index()
  
  #subset negative keywords
  negative_kws_pre_de_dupe = all_search_queries[(all_search_queries['taps'] >= KAP["NEGATIVE_KEYWORD_TAP_THRESHOLD"]) & (all_search_queries['installs'] <= KAP["NEGATIVE_KEYWORD_CONVERSION_THRESHOLD"])]
  
  #subset targeted keywords
  targeted_kws_pre_de_dupe = all_search_queries[(all_search_queries['taps'] >= KAP["TARGETED_KEYWORD_TAP_THRESHOLD"]) & (all_search_queries['installs'] >= KAP["TARGETED_KEYWORD_CONVERSION_THRESHOLD"])]
  
  #get negative keyword text only before de-duping
  negative_kws_pre_de_dupe_text_only_first_step = negative_kws_pre_de_dupe['searchTermText']
  negative_kws_pre_de_dupe_text_only_second_step = negative_kws_pre_de_dupe_text_only_first_step[negative_kws_pre_de_dupe_text_only_first_step != 'none']
  
  #get targeted keyword text only before de-duping
  targeted_kws_pre_de_dupe_text_only_first_step = targeted_kws_pre_de_dupe['searchTermText']
  targeted_kws_pre_de_dupe_text_only_second_step = targeted_kws_pre_de_dupe_text_only_first_step[targeted_kws_pre_de_dupe_text_only_first_step != 'none']
  
  return analyzeKeywordsSharedCode(
    KAP,
    targeted_kws_pre_de_dupe_text_only_second_step,
    negative_kws_pre_de_dupe_text_only_second_step,
    ids["campaignId"]["search"],
    ids["campaignId"]["broad"],
    ids["campaignId"]["exact"],
    ids["adGroupId"]["search"],
    ids["adGroupId"]["broad"],
    ids["adGroupId"]["exact"],
    currency
  )


@retry
def sendNonDuplicatesToAppleHelper(url, cert, data, headers):
  return requests.post(
    url, 
    cert=cert, 
    data=data, 
    headers=headers, 
    timeout=config.HTTP_REQUEST_TIMEOUT
  )

# @debug
def sendNonDuplicatesToApple(client, url, payload, headers, duplicateKeywordIndices):
  payloadPy = json.loads(payload)
  
  newPayload = [payloadPy[index] for index in range(len(payloadPy)) \
                if index not in duplicateKeywordIndices]

  dprint("About to send non-duplicates payload %s." % pprint.pformat(newPayload))
  response = sendNonDuplicatesToAppleHelper(
    url,
    cert=(S3Utils.getCert(client.pemFilename), S3Utils.getCert(client.keyFilename)),
    data=json.dumps(newPayload),
    headers=headers
  )

  if response.status_code == 200:
    dprint("NonDuplicate send worked.");

  else:
    email = "client id:%d \n url:%s \n response:%s" % (client.orgId, url, response)
    date = time.strftime("%m/%d/%Y")
    subject ="%s:%d ERROR in runKeywordAdder for %s" % (date, response.status_code, client.clientName)
    logger.warn(email)
    logger.error(subject)
    if sendG:
      EmailUtils.sendTextEmail(email, subject, EMAIL_TO, [], config.EMAIL_FROM)
       
  return response


@retry
def sendToAppleHelper(url, cert, data, headers):
  return requests.post(
    url, 
    cert=cert, 
    data=data, 
    headers=headers, 
    timeout=config.HTTP_REQUEST_TIMEOUT
  )

def sendToApple(client, payloads):
    headers = { "Authorization": "orgId=%s" % client.orgId, "Content-Type" : "application/json", "Accept" : "application/json",}
    if sendG:
        responses = []
        for payload in payloads:
            appleEndpointUrl = payload[1]
            payloadForPost = payload[0]
            dprint("runKeywordAdder:::sendToApple:::Payload: '%s'" % payloadForPost)
            dprint("runKeywordAdder:::sendToApple:::appleEndpointUrl: '%s'" % appleEndpointUrl)

            response = sendToAppleHelper(
              appleEndpointUrl,
              cert=(S3Utils.getCert(client.pemFilename), S3Utils.getCert(client.keyFilename)),
              data=payloadForPost,
              headers=headers
            )

            if response.status_code == 200:
                continue

            if response.status_code != 400:
                print("WARNING: Error %s should be 400 from Apple URL '%s'.  Response of type '%s' is %s." % \
                    (response.status_code, appleEndpointUrl, response.headers["Content-Type"], response.text))
                continue
       
            if response.headers["Content-Type"] not in JSON_MIME_TYPES:
                print("WARNING: Error %s from Apple URL '%s'.  Response of type '%s' is %s; should be one of %s." % \
                    (response.status_code, appleEndpointUrl, response.headers["Content-Type"], response.text, JSON_MIME_TYPES))
                continue

            # Response from Apple is a JSON object like this:
            #
            # {"data"       : null,
            #  "pagination" : null,
            #  "error"      : {"errors" : [ {"messageCode" : "DUPLICATE_KEYWORD",
            #                                "message"     : "duplicate keyword text",
            #                                "field"       : "NegativeKeywordImport[0].text"},
            #                               {"messageCode" : "DUPLICATE_KEYWORD",
            #                                "message"     : "duplicate keyword text",
            #                                "field"       : "NegativeKeywordImport[1].text"}
            #                             ]
            #                 }
            # }

            # Or this Pythonic version:
            # {'data': None,
            #  'error': {'errors': [{'field'      : 'KeywordImport[0].text',
            #                        'message'    : 'duplicate keyword text',
            #                        'messageCode': 'DUPLICATE_KEYWORD'},
            #                       {'field'      : 'KeywordImport[1].text',
            #                        'message'    : 'duplicate keyword text',
            #                        'messageCode': 'DUPLICATE_KEYWORD'}]},
            #  'pagination': None}

            errorObject = response.json()
            dprint("errorObject is %s" % pprint.pformat(errorObject))

            if "error" not in errorObject:
                print("WARNING: Missing 'error' attribute in response (%s) from Apple URL '%s'. Response is %s." % (response.status_code, appleEndpointUrl, pprint.pformat(errorObject)))
                continue

            errorsSubObject = errorObject["error"]

            if "errors" not in errorsSubObject:
                print("WARNING: Missing 'errors' SUBattribute in response (%s) from Apple URL '%s'. Response is %s." % (response.status_code, appleEndpointUrl, pprint.pformat(errorsSubObject)))
                continue

            errors = errorsSubObject["errors"]

            if type(errors) != list:
                print("WARNING: 'errors' isn't an array (a Python list) in response (%s) from Apple URL '%s'. Response is of type %s and is %s." % (response.status_code, appleEndpointUrl, type(errors), pprint.pformat(errors)))
                continue

            duplicateKeywordIndices = set()

            for error in errors:
                if type(error) != dict:
                    print("WARNING: error object isn't an hashmap (a Python dict) in response (%s) from Apple URL '%s'. It is of type %s and is %s." % (response.status_code, appleEndpointUrl, type(error), pprint.pformat(error)))
                    continue

                messageCode, message, field = error.get("messageCode"), error.get("message"), error.get("field")

                if messageCode == None or message == None or field == None:
                    print("WARNING: error message is missing one or more of 'messageCode,' 'message,' and 'field' attributes in response (%s) from Apple URL '%s'. It is %s." % (response.status_code, appleEndpointUrl, pprint.pformat(error)))
                    continue

                # TODO: Centralize the repetition of "duplicated keyword text" in the test and error message. --DS, 26-Oct-2018
                DUPLICATE_KEYWORD_UPPERCASE = "DUPLICATE_KEYWORD"
                DUPLICATE_KEYWORD_LOWERCASE = ("duplicate keyword text", "duplicated keyword")
                if messageCode != DUPLICATE_KEYWORD_UPPERCASE or message.lower() not in DUPLICATE_KEYWORD_LOWERCASE:
                    print("WARNING: messageCode '%s' isn't '%s' and/or message (lowercased) '%s' isn't in %s in response (%s) from Apple URL '%s'. It is %s for error message %s." % (messageCode,
                                          DUPLICATE_KEYWORD_UPPERCASE,
                                          message.lower(),
                                          DUPLICATE_KEYWORD_LOWERCASE,
                                          response.status_code,
                                          appleEndpointUrl,
                                          pprint.pformat(messageCode),
                                          error))
                    continue

                indexMatch = DUPLICATE_KEYWORD_REGEX.match(field)

                if indexMatch == None:
                    print("WARNING: field with array index didn't match regular expression '%s' in response (%s) from Apple URL '%s'. It is %s for error message %s." % (DUPLICATE_KEYWORD_REGEX.pattern, response.status_code, appleEndpointUrl, pprint.pformat(messageCode), error))
                    continue

                duplicateKeywordIndices.add(int(indexMatch.group("index")))

                # If there were no errors, keep the response. Otherwise, throw the
                # response away and use the response from sending the non-duplicates.
                # This means that the response from a partially-successful update will
                # be lost.
                if len(duplicateKeywordIndices) == 0:
                    responses.append(response)

                else:
                    responses.append(sendNonDuplicatesToApple(client, appleEndpointUrl, payload, headers, duplicateKeywordIndices))

                response = "\n".join(["%s: %s" % (response.status_code, response.text) for response in responses])

    else:
        response = "Not actually sending anything to Apple."
        for payload in payloads:
            print("runKeywordAdder:::sendToApple-false:::payload:'" + str(payload[0]))
            print("runKeywordAdder:::sendToApple-false:::url:'" + str(payload[1]))

        dprint("The result of sending the keywords to Apple: %s" % response)

    return sendG


def createEmailBody(data, sent):
  content = [
    """Keywords Added Report""",
    """Sent to Apple is %s.""" % sent,
  ]

  for client, clientData in data.items():
    content.append(client)
    for item in (
      ("+e", "Exact Matches Added"),
      ("+b", "Broad Matches Added"),
      ("-e", "Exact Negative Matches Added"),
      ("-b", "Broad Negative Matches Added")
    ):
      content.append(item[1])

      content.append("""\n""".join([keyword["text"] for keyword in clientData[item[0]]]))

  return "\n".join(content)


def emailSummaryReport(data, sent):
    messageString = createEmailBody(data, sent);
    dateString = time.strftime("%m/%d/%Y")
    if dateString.startswith("0"):
        dateString = dateString[1:]
    subjectString ="Keyword Adder summary for %s" % dateString
    EmailUtils.sendTextEmail(messageString, subjectString, EMAIL_TO, [], config.EMAIL_FROM)


def convertAnalysisIntoApplePayloadAndSend(
  client,
  CSRI,
  exactPositive,
  exactPositiveUrl,
  broadPositive,
  broadPositiveUrl,
  exactNegative,
  exactNegativeUrl,
  broadNegative,
  broadNegativeUrl
):
    CSRI["+e"] = json.loads(exactPositive)
    CSRI["+b"] = json.loads(broadPositive)
    CSRI["-e"] = json.loads(exactNegative)
    CSRI["-b"] = json.loads(broadNegative)

    exactPositiveText = [item["text"] for item in CSRI["+e"]]
    broadPositiveText = [item["text"] for item in CSRI["+b"]]
    exactNegativeText = [item["text"] for item in CSRI["-e"]]
    broadNegativeText = [item["text"] for item in CSRI["-b"]]

#    if len(set(exactPositiveText + broadPositiveText)) != len(exactPositiveText) + len(broadPositiveText):
#      print("ERROR: There are identical text strings in the exact and broad positive matches.  They are %s and %s." % (exactPositiveText, broadPositiveText))
#
#    if len(set(exactNegativeText + broadNegativeText)) != len(exactNegativeText) + len(broadNegativeText):
#      print("ERROR: There are identical text strings in the exact and broad negative matches.  They are %s and %s." % (exactNegativeText, broadNegativeText))

    sent = sendToApple(client, ((exactPositive, exactPositiveUrl), (broadPositive, broadPositiveUrl)))
    sendToApple(client, ((exactNegative, exactNegativeUrl), (broadNegative, broadNegativeUrl)))

    # JF release-1 airlift bid counts and keywords to dynamo
    client.writePositiveKeywordsAdded(dynamodb, exactPositiveText + broadPositiveText)
    client.writeNegativeKeywordsAdded(dynamodb, exactNegativeText + broadNegativeText)
    return sent


# @debug
def process():
  summaryReportInfo = { }

  for client in clientsG:
    summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = CSRI = { }

    kAI = client.keywordAdderIds
    searchCampaignId, broadCampaignId = kAI["campaignId"]["search"], kAI["campaignId"]["broad"]

    searchMatchData = getSearchTermsReportFromApple(client, searchCampaignId)
    broadMatchData  = getSearchTermsReportFromApple(client, broadCampaignId)

    exactPositive, exactPositiveUrl, broadPositive, broadPositiveUrl, exactNegative, exactNegativeUrl, broadNegative, broadNegativeUrl = \
      analyzeKeywords(searchMatchData, broadMatchData, kAI, client.keywordAdderParameters, client.currency)

    sent = convertAnalysisIntoApplePayloadAndSend(
      client,
      CSRI,
      exactPositive,
      exactPositiveUrl,
      broadPositive,
      broadPositiveUrl,
      exactNegative,
      exactNegativeUrl,
      broadNegative,
      broadNegativeUrl
    )
    
  emailSummaryReport(summaryReportInfo, sent)


# @debug
def terminate():
  pass


if __name__ == "__main__":
    initialize('lcl', 'http://localhost:8000', ["james@adoya.io"])
    process()
    terminate()


def lambda_handler(event, context):
    initialize(event['env'], event['dynamoEndpoint'], event['emailToInternal'])
    process()
    terminate()
    return {
        'statusCode': 200,
        'body': json.dumps('Run Keyword Adder Complete')
    }