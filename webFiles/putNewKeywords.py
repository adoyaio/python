#! /usr/bin/python3
#! MACINTOSH /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import json
import os
import pandas as pd
import sys
import time
import urllib.parse

if sys.platform == "linux":
  item = "/home/scott/ScottKaplan/Adoya"
  os.chdir(item)
  sys.path.append(item)

elif sys.platform == "darwin":
  item = "/Users/Davidschachter/Scott Kaplan/2018_10_01"
  os.chdir(item)
  sys.path.append(item)


from runKeywordAdder import analyzeKeywordsSharedCode, \
                            convertAnalysisIntoApplePayloadAndSend, \
                            enableSendingToApple

from Client import CLIENTS
from debug import debug, dprint, disableDebug; disableDebug()

# TODO: Add password checking and https encryption. --DS, 23-Aug-2018



# ------------------------------------------------------------------------------
@debug
def updateClient(client, keywords):
  dprint(keywords)
  kAI = client.keywordAdderIds

  kAP = client.keywordAdderParameters

  # --- COPIED FROM SCOTT'S CODE ---
  #this part converts list to dataframe
  input_kws_second_step = pd.DataFrame(keywords)

  #this part provides the column header
  input_kws_second_step.columns = ['text']

  #this part takes the input keywords and assigns them to the targeted and negative keyword variables
  targeted_kws_pre_de_dupe_text_only_second_step = input_kws_second_step.copy()
  negative_kws_pre_de_dupe_text_only_second_step = input_kws_second_step.copy()
  # --- END COPIED SECTION ---

  exactPositive, broadPositive, exactNegative, broadNegative = \
    analyzeKeywordsSharedCode(kAP,
                              targeted_kws_pre_de_dupe_text_only_second_step,
                              negative_kws_pre_de_dupe_text_only_second_step,
                              kAI["campaignId"]["search"],
                              kAI["campaignId"]["broad"],
                              kAI["campaignId"]["exact"],
                              kAI["adGroupId"]["search"],
                              kAI["adGroupId"]["broad"],
                              kAI["adGroupId"]["exact"])

  CSRI = { }
  convertAnalysisIntoApplePayloadAndSend(client,
                                         CSRI,
                                         exactPositive,
                                         broadPositive,
                                         exactNegative,
                                         broadNegative)

  return CSRI
 


# ------------------------------------------------------------------------------
@debug
def makeHTML(client, CSRI):
  response = [ """<table><caption>%s (%s)</caption>""" % (client.clientName, client.orgId) ]

  READABLE = {"+e" : "Exact Positive",
              "-e" : "Exact Negative",
              "+b" : "Broad Positive",
              "-b" : "Broad negative"
             }

  for k, v in CSRI.items():
    values = "\n".join(["""%s<br>""" % item["text"] for item in v])
    response.append("""<tr><td valign="top">%s</td><td>%s</td></tr>""" % (READABLE[k], values))

  response.append("""</table>""")

  return "\n".join(response)
 


# ------------------------------------------------------------------------------
@debug
def runProgram():
  enableSendingToApple()

  content = [
    """<html><head><title>Adoya Keyword Adder</title>""",
    """<style>h1, h2 { font-family: sans-serif; }</style>"""
    """</head>""",
    """<body><h1><img src="/adoyalogo.png" border="0">Keyword Adder</h1>""",
  ]

  clients = {client.orgId : client for client in CLIENTS}

  webData = urllib.parse.parse_qs(sys.stdin.read(int(os.environ["CONTENT_LENGTH"])))

  dprint(webData)

  content.extend((makeHTML(clients[int(key)], updateClient(clients[int(key)], value[0].strip().split('\r\n'))) \
                  for key, value in webData.items() \
                  if key != "Save"))

  content.append("""<h2>Data Updated</h2><a href="/cgi-bin/getNewKeywords.py">Continue</a></body></html>""")

  result = "\n".join(content)
  print("Content-Type: text/html")
  print("Content-Length: %s" % len(result))
  print()
  print(result, end="")



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  runProgram()
