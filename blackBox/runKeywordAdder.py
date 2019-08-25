#! /usr/bin/python3

from collections import defaultdict
import datetime
import email.message
from email.headerregistry import Address
import json
import os
#import numpy as np
import pandas as pd
import pprint
import re
import requests
import smtplib 
import sys
import time
import urllib.parse

if sys.platform == "linux":
  os.chdir("/home/scott/ScottKaplan/Adoya")

elif sys.platform == "darwin":
  os.chdir("/Users/Davidschachter/Scott Kaplan/2018_10_01")

else:
  PATH_ADDEND = "C:\\Users\A\\Desktop\\apple_search_ads_api\\keyword_adder"
  print("Adding '%s' to sys.path." % PATH_ADDEND);
  sys.path.append(PATH_ADDEND)

from Client import CLIENTS
from configuration import SMTP_HOSTNAME, \
                          SMTP_PORT, \
                          SMTP_USERNAME, \
                          SMTP_PASSWORD, \
                          EMAIL_FROM, \
                          APPLE_KEYWORD_SEARCH_TERMS_URL_TEMPLATE, \
                          APPLE_UPDATE_POSITIVE_KEYWORDS_URL, \
                          APPLE_UPDATE_NEGATIVE_KEYWORDS_URL, \
                          HTTP_REQUEST_TIMEOUT

from debug import debug, dprint
from retry import retry


EMAIL_TO         = (Address("David Schachter", "davidschachter", "gmail.com"),
                    Address("Scott Kaplan",    "scott.kaplan",   "ssjdigital.com"),
                   )
JSON_MIME_TYPES  = ("application/json", "text/json")

DUPLICATE_KEYWORD_REGEX = re.compile("(NegativeKeywordImport|KeywordImport)\[(?P<index>\d+)\]\.text")

###### date and time parameters for bidding lookback ######
date = datetime.date
today = datetime.date.today()
end_date_delta = datetime.timedelta(days=1)
start_date_delta = datetime.timedelta(days=365)
start_date = today - start_date_delta
end_date = today - end_date_delta


sendG = False # Set to True to enable sending data to Apple, else a test run.



# ------------------------------------------------------------------------------
@debug
def enableSendingToApple():
  global sendG


  sendG = True



# ------------------------------------------------------------------------------
@debug
def initialize():
  if "-s" in sys.argv or "--send" in sys.argv:
    enableSendingToApple()

  dprint("In initialize(), getcwd()='%s' and sendG=%s." % (os.getcwd(), sendG))



# ------------------------------------------------------------------------------
@retry
def getSearchTermsReportFromAppleHelper(url, cert, json, headers):
  return requests.post(url, cert=cert, json=json, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)



# ------------------------------------------------------------------------------
@debug
def getSearchTermsReportFromApple(client, campaignId):
  payload = { "startTime"                  : str(start_date), 
              "endTime"                    : str(end_date),
              #"granularity"                : 2, # 1=hourly, 2=daily, 3=monthly, etc.
              "selector"                   : { "orderBy"    : [ { "field"     : "localSpend",
                                                                  "sortOrder" : "DESCENDING"
                                                                } ], 
                                               "fields"     :  [ "localSpend",
                                                                 "taps",
                                                                 "impressions",
                                                                 "conversions",
                                                                 "avgCPA",
                                                                 "avgCPT",
                                                                 "ttr",
                                                                 "conversionRate"
                                                               ],
                                               "pagination" : { "offset" : 0,
                                                                "limit"  : 1000
                                                              }
                                             },
              #"groupBy"                    : ["COUNTRY_CODE"], 
              "returnRowTotals"            : True, 
              "returnRecordsWithNoMetrics" : False
            }
  url = APPLE_KEYWORD_SEARCH_TERMS_URL_TEMPLATE % campaignId

  headers = { "Authorization": "orgId=%s" % client.orgId }

  dprint ("URL is '%s'." % url)
  dprint ("Payload is '%s'." % payload)
  dprint ("Headers are %s." % headers)

  response = getSearchTermsReportFromAppleHelper(url,
                                                 cert=(client.pemPathname, client.keyPathname),
                                                 json=payload,
                                                 headers=headers)
  dprint ("Response is %s." % response)

  return json.loads(response.text) 



# ------------------------------------------------------------------------------
@debug
def analyzeKeywordsSharedCode(KAP,
                              targeted_kws_pre_de_dupe_text_only_second_step,
                              negative_kws_pre_de_dupe_text_only_second_step,
                              search_match_campaign_id,
                              broad_match_campaign_id,
                              exact_match_campaign_id,
                              search_match_ad_group_id,
                              broad_match_ad_group_id,
                              exact_match_ad_group_id):
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
  search_match_negatives_df['importAction'] = search_match_negatives_df.shape[0]*['CREATE']
  
  #add campaign id column and update value as per apple search api requirement
  search_match_negatives_df['campaignId'] = search_match_negatives_df.shape[0]*[search_match_campaign_id]
  
  #add ad group id column and update value as per apple search api requirement
  search_match_negatives_df['adGroupId'] = search_match_negatives_df.shape[0]*[search_match_ad_group_id]
  
  #add match type column and update value as per apple search api requirement
  search_match_negatives_df['matchType'] = search_match_negatives_df.shape[0]*['EXACT']
  
  #add status column and update value as per apple search api requirement
  search_match_negatives_df['status'] = search_match_negatives_df.shape[0]*['ACTIVE']
  
  #create dataframe for broad match negatives
  #add action type column and update value as per apple broad api requirement
  broad_match_negatives_df['importAction'] = broad_match_negatives_df.shape[0]*['CREATE']
  
  #add campaign id column and update value as per apple broad api requirement
  broad_match_negatives_df['campaignId'] = broad_match_negatives_df.shape[0]*[broad_match_campaign_id]
  
  #add ad group id column and update value as per apple broad api requirement
  broad_match_negatives_df['adGroupId'] = broad_match_negatives_df.shape[0]*[broad_match_ad_group_id]
  
  #add match type column and update value as per apple broad api requirement
  broad_match_negatives_df['matchType'] = broad_match_negatives_df.shape[0]*['EXACT']
  
  #add status column and update value as per apple search api requirement
  broad_match_negatives_df['status'] = broad_match_negatives_df.shape[0]*['ACTIVE']
  
  #convert search and broad match negative dataframes into jsons for uploading
  search_match_negatives_for_upload = search_match_negatives_df.to_json(orient = 'records')
  broad_match_negatives_for_upload = broad_match_negatives_df.to_json(orient = 'records')
  
  #create dataframe for targeted keywords
  #update column name for targeted keywords & convert into dataframe
  targeted_kws_pre_de_dupe_text_only_third_step_df = pd.DataFrame(targeted_kws_pre_de_dupe_text_only_second_step)
  targeted_kws_pre_de_dupe_text_only_fourth_step_df = targeted_kws_pre_de_dupe_text_only_third_step_df.rename(columns={'searchTermText': 'text'})
  
  #create separate variables for targeted exact and broad match additions
  exact_match_targeted_first_step_df = targeted_kws_pre_de_dupe_text_only_fourth_step_df.copy()
  broad_match_targeted_first_step_df = targeted_kws_pre_de_dupe_text_only_fourth_step_df.copy()
  
  #create exact match keyword file for uploading
  #add action type column and update value as per apple broad api requirement
  exact_match_targeted_first_step_df['importAction'] = exact_match_targeted_first_step_df.shape[0]*['CREATE']
  
  #add match type column and update value as per apple broad api requirement
  exact_match_targeted_first_step_df['matchType'] = exact_match_targeted_first_step_df.shape[0]*['EXACT']
  
  #add match type column and update value as per apple broad api requirement
  exact_match_targeted_first_step_df['status'] = exact_match_targeted_first_step_df.shape[0]*['ACTIVE']
  
  #add campaign id column and update value as per apple search api requirement
  exact_match_targeted_first_step_df['campaignId'] = exact_match_targeted_first_step_df.shape[0]*[exact_match_campaign_id]
  
  #add ad group id column and update value as per apple search api requirement
  exact_match_targeted_first_step_df['adGroupId'] = exact_match_targeted_first_step_df.shape[0]*[exact_match_ad_group_id]
  
  #add bid column and update value as per apple search api requirement
  exact_match_targeted_first_step_df['bidAmount'] = exact_match_targeted_first_step_df.shape[0]*[KAP["EXACT_MATCH_DEFAULT_BID"]]
  
  #add bid column and update value as per apple search api requirement
  exact_match_targeted_first_step_df['bidAmount'] = exact_match_targeted_first_step_df.shape[0]*[{"amount":""+str(KAP["EXACT_MATCH_DEFAULT_BID"]), "currency":"USD"}]
  
  #create broad match keyword file for uploading
  #add action type column and update value as per apple broad api requirement
  broad_match_targeted_first_step_df['importAction'] = broad_match_targeted_first_step_df.shape[0]*['CREATE']
  
  #add match type column and update value as per apple broad api requirement
  broad_match_targeted_first_step_df['matchType'] = broad_match_targeted_first_step_df.shape[0]*['BROAD']
  
  #add match type column and update value as per apple broad api requirement
  broad_match_targeted_first_step_df['status'] = broad_match_targeted_first_step_df.shape[0]*['ACTIVE']
  
  #add campaign id column and update value as per apple search api requirement
  broad_match_targeted_first_step_df['campaignId'] = broad_match_targeted_first_step_df.shape[0]*[broad_match_campaign_id]
  
  #add ad group id column and update value as per apple search api requirement
  broad_match_targeted_first_step_df['adGroupId'] = broad_match_targeted_first_step_df.shape[0]*[broad_match_ad_group_id]
  
  #add bid column and update value as per apple search api requirement
  broad_match_targeted_first_step_df['bidAmount'] = broad_match_targeted_first_step_df.shape[0]*[KAP["BROAD_MATCH_DEFAULT_BID"]]
  
  #add bid column and update value as per apple search api requirement
  broad_match_targeted_first_step_df['bidAmount'] = broad_match_targeted_first_step_df.shape[0]*[{"amount":""+str(KAP["BROAD_MATCH_DEFAULT_BID"]), "currency":"USD"}]
  
  #convert search and broad match targeted dataframes into jsons for uploading
  exact_match_targeted_for_upload = exact_match_targeted_first_step_df.to_json(orient = 'records')
  broad_match_targeted_for_upload = broad_match_targeted_first_step_df.to_json(orient = 'records')
  
  return exact_match_targeted_for_upload, \
         broad_match_targeted_for_upload, \
         search_match_negatives_for_upload, \
         broad_match_negatives_for_upload



# ------------------------------------------------------------------------------
@debug
def analyzeKeywords(search_match_data, broad_match_data, ids, keywordAdderParameters):
  KAP = keywordAdderParameters;
  
  #######mine search match search queries#######
  
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
      search_match_extract_third_step['conversions'].append(r['total']['conversions'])
      search_match_extract_third_step['conversionsNewDownloads'].append(r['total']['conversionsNewDownloads'])
      search_match_extract_third_step['conversionsRedownloads'].append(r['total']['conversionsRedownloads'])
      search_match_extract_third_step['conversionsLATOn'].append(r['total']['conversionsLATOn'])
      search_match_extract_third_step['conversionsLATOff'].append(r['total']['conversionsLATOff'])
      search_match_extract_third_step['avgCPA'].append(r['total']['avgCPA']['amount'])
      search_match_extract_third_step['conversionRate'].append(r['total']['conversionRate'])
      search_match_extract_third_step['localSpend'].append(r['total']['localSpend']['amount'])	
      search_match_extract_third_step['avgCPT'].append(r['total']['avgCPT']['amount'])
  
  #convert to dataframe    
  search_match_extract_df = pd.DataFrame(search_match_extract_third_step)
  
  #######mine broad match broad queries#######
  
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
    broad_match_extract_third_step['conversions'].append(r['total']['conversions'])
    broad_match_extract_third_step['conversionsNewDownloads'].append(r['total']['conversionsNewDownloads'])
    broad_match_extract_third_step['conversionsRedownloads'].append(r['total']['conversionsRedownloads'])
    broad_match_extract_third_step['conversionsLATOn'].append(r['total']['conversionsLATOn'])
    broad_match_extract_third_step['conversionsLATOff'].append(r['total']['conversionsLATOff'])
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
  all_search_queries = all_match_type_combine_second_step_df.groupby('searchTermText')['conversions','taps'].sum().reset_index()
  
  #subset negative keywords
  negative_kws_pre_de_dupe = all_search_queries[(all_search_queries['taps'] >= KAP["NEGATIVE_KEYWORD_TAP_THRESHOLD"]) & (all_search_queries['conversions'] <= KAP["NEGATIVE_KEYWORD_CONVERSION_THRESHOLD"])]
  
  #subset targeted keywords
  targeted_kws_pre_de_dupe = all_search_queries[(all_search_queries['taps'] >= KAP["TARGETED_KEYWORD_TAP_THRESHOLD"]) & (all_search_queries['conversions'] >= KAP["TARGETED_KEYWORD_CONVERSION_THRESHOLD"])]
  
  #get negative keyword text only before de-duping
  negative_kws_pre_de_dupe_text_only_first_step = negative_kws_pre_de_dupe['searchTermText']
  negative_kws_pre_de_dupe_text_only_second_step = negative_kws_pre_de_dupe_text_only_first_step[negative_kws_pre_de_dupe_text_only_first_step != 'none']
  
  #get targeted keyword text only before de-duping
  targeted_kws_pre_de_dupe_text_only_first_step = targeted_kws_pre_de_dupe['searchTermText']
  targeted_kws_pre_de_dupe_text_only_second_step = targeted_kws_pre_de_dupe_text_only_first_step[targeted_kws_pre_de_dupe_text_only_first_step != 'none']
  
  return analyzeKeywordsSharedCode(KAP,
                                   targeted_kws_pre_de_dupe_text_only_second_step,
                                   negative_kws_pre_de_dupe_text_only_second_step,
                                   ids["campaignId"]["search"],
                                   ids["campaignId"]["broad"],
                                   ids["campaignId"]["exact"],
                                   ids["adGroupId"]["search"],
                                   ids["adGroupId"]["broad"],
                                   ids["adGroupId"]["exact"])



# ------------------------------------------------------------------------------
@retry
def sendNonDuplicatesToAppleHelper(url, cert, data, headers):
  return requests.post(url, cert=cert, data=data, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)



# ------------------------------------------------------------------------------
@debug
def sendNonDuplicatesToApple(client, url, payload, headers, duplicateKeywordIndices):
  payloadPy = json.loads(payload)
  
  newPayload = [payloadPy[index] for index in range(len(payloadPy)) \
                if index not in duplicateKeywordIndices]

  dprint("About to send non-duplicates payload %s." % pprint.pformat(newPayload))

  response = sendNonDuplicatesToAppleHelper(url,
                                            cert=(client.pemPathname, client.keyPathname),
                                            data=json.dumps(newPayload),
                                            headers=headers)

  if response.status_code == 200:
    dprint("NonDuplicate send worked.");

  else:
    print("WARNING: Error %s received from Apple URL '%s'.  Response of type '%s' is %s." % \
            (response.status_code, url, response.headers["Content-Type"], response.text))
       
  return response



# ------------------------------------------------------------------------------
@retry
def sendToAppleHelper(url, cert, data, headers):
  return requests.post(url, cert=cert, data=data, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)



# ------------------------------------------------------------------------------
@debug
def sendToApple(client, url, payloads):
  headers = { "Authorization": "orgId=%s" % client.orgId,
              "Content-Type" : "application/json",
              "Accept"       : "application/json",
            }

  if sendG:
    responses = []
    for payload in payloads:
      dprint("Payload: '%s'" % payload)
      response = sendToAppleHelper(url,
                                   cert=(client.pemPathname, client.keyPathname),
                                   data=payload,
                                   headers=headers)

      if response.status_code == 200:
        continue

      if response.status_code != 400:
        print("WARNING: Error %s should be 400 from Apple URL '%s'.  Response of type '%s' is %s." % \
                (response.status_code, url, response.headers["Content-Type"], response.text))
        continue
       
      if response.headers["Content-Type"] not in JSON_MIME_TYPES:
        print("WARNING: Error %s from Apple URL '%s'.  Response of type '%s' is %s; should be one of %s." % \
                (response.status_code, url, response.headers["Content-Type"], response.text, JSON_MIME_TYPES))
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
        print("WARNING: Missing 'error' attribute in response (%s) from Apple URL '%s'. Response is %s." % (response.status_code, url, pprint.pformat(errorObject)))
        continue

      errorsSubObject = errorObject["error"] 

      if "errors" not in errorsSubObject:
        print("WARNING: Missing 'errors' SUBattribute in response (%s) from Apple URL '%s'. Response is %s." % (response.status_code, url, pprint.pformat(errorsSubObject)))
        continue

      errors = errorsSubObject["errors"]

      if type(errors) != list:
        print("WARNING: 'errors' isn't an array (a Python list) in response (%s) from Apple URL '%s'. Response is of type %s and is %s." % (response.status_code, url, type(errors), pprint.pformat(errors)))
        continue

      duplicateKeywordIndices = set()

      for error in errors:
        if type(error) != dict:
          print("WARNING: error object isn't an hashmap (a Python dict) in response (%s) from Apple URL '%s'. It is of type %s and is %s." % (response.status_code, url, type(error), pprint.pformat(error)))
          continue

        messageCode, message, field = error.get("messageCode"), error.get("message"), error.get("field")

        if messageCode == None or message == None or field == None:
          print("WARNING: error message is missing one or more of 'messageCode,' 'message,' and 'field' attributes in response (%s) from Apple URL '%s'. It is %s." % (response.status_code, url, pprint.pformat(error)))
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
                                          url,
                                          pprint.pformat(messageCode),
                                          error))
          continue

        indexMatch = DUPLICATE_KEYWORD_REGEX.match(field)

        if indexMatch == None:
          print("WARNING: field with array index didn't match regular expression '%s' in response (%s) from Apple URL '%s'. It is %s for error message %s." % (DUPLICATE_KEYWORD_REGEXP.pattern, response.status_code, url, pprint.pformat(messageCode), error))
          continue

        duplicateKeywordIndices.add(int(indexMatch.group("index")))

      # If there were no errors, keep the response. Otherwise, throw the
      # response away and use the response from sending the non-duplicates.
      # This means that the response from a partially-successful update will
      # be lost.
      if len(duplicateKeywordIndices) == 0:
        responses.append(response)

      else:
        responses.append(sendNonDuplicatesToApple(client, url, payload, headers, duplicateKeywordIndices))


    response = "\n".join(["%s: %s" % (response.status_code, response.text) for response in responses])

  else:
    response = "Not actually sending anything to Apple."

  dprint("The result of sending the keywords to Apple: %s" % response)

  return sendG



# ------------------------------------------------------------------------------
@debug
def createEmailBody(data, sent):
  content = ["""Keywords Added Report""",
             """Sent to Apple is %s.""" % sent,
            ]

  for client, clientData in data.items():
    content.append(client)
    for item in (("+e", "Exact Matches Added"),
                 ("+b", "Broad Matches Added"),
                 ("-e", "Exact Negative Matches Added"),
                 ("-b", "Broad Negative Matches Added")):
      content.append(item[1])

      content.append("""\n""".join([keyword["text"] for keyword in clientData[item[0]]]))

  return "\n".join(content)



# ------------------------------------------------------------------------------
@debug
def emailSummaryReport(data, sent):
  msg = email.message.EmailMessage()
  msg.set_content(createEmailBody(data, sent))

  dateString = time.strftime("%m/%d/%Y")
  if dateString.startswith("0"):
    dateString = dateString[1:]

  msg['Subject'] = "Keyword Adder summary for %s" % dateString
  msg['From']    = EMAIL_FROM
  msg['To']      = EMAIL_TO
#  msg.replace_header("Content-Type", "text/html")


  # TODO: Merge this duplicate code with runClientDailyReports.py. --DS, 30-Aug-2018
  if sys.platform == "linux": # Don't try to send email on Scott's "Windows" box.
    dprint("SMTP hostname/port=%s/%s" % (SMTP_HOSTNAME, SMTP_PORT))

    with smtplib.SMTP(host=SMTP_HOSTNAME, port=SMTP_PORT) as smtpServer:
      smtpServer.set_debuglevel(2)
      smtpServer.starttls()
      smtpServer.login(SMTP_USERNAME, SMTP_PASSWORD)
      smtpServer.send_message(msg)



# ------------------------------------------------------------------------------
@debug
def convertAnalysisIntoApplePayloadAndSend(client,
                                           CSRI,
                                           exactPositive,
                                           broadPositive,
                                           exactNegative,
                                           broadNegative):
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

    sent = sendToApple(client, APPLE_UPDATE_POSITIVE_KEYWORDS_URL, (exactPositive, broadPositive))
    sendToApple(client, APPLE_UPDATE_NEGATIVE_KEYWORDS_URL, (exactNegative, broadNegative))

    client.positiveKeywordsAdded = exactPositiveText + broadPositiveText
    client.negativeKeywordsAdded = exactNegativeText + broadNegativeText

    return sent



# ------------------------------------------------------------------------------
@debug
def process():
  summaryReportInfo = { }

  for client in CLIENTS:
    summaryReportInfo["%s (%s)" % (client.orgId, client.clientName)] = CSRI = { }

    kAI = client.keywordAdderIds
    searchCampaignId, broadCampaignId = kAI["campaignId"]["search"], kAI["campaignId"]["broad"]

    searchMatchData = getSearchTermsReportFromApple(client, searchCampaignId)
    broadMatchData  = getSearchTermsReportFromApple(client, broadCampaignId)

    exactPositive, broadPositive, exactNegative, broadNegative = \
      analyzeKeywords(searchMatchData, broadMatchData, kAI, client.keywordAdderParameters)

    sent = convertAnalysisIntoApplePayloadAndSend(client,
                                                  CSRI,
                                                  exactPositive,
                                                  broadPositive,
                                                  exactNegative,
                                                  broadNegative)

  emailSummaryReport(summaryReportInfo, sent)



# ------------------------------------------------------------------------------
@debug
def terminate():
  pass



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  initialize()
  process()
  terminate()
