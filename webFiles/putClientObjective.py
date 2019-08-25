#! /usr/bin/python3
#! MACINTOSH /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import email.message
from email.headerregistry import Address
import json
import os
import re
import smtplib
import sys
import time
import traceback
import urllib.parse

from cgiConfiguration import DATA_DIR, \
                             HTTP_STATUS_OK, \
                             HTTP_STATUS_303, \
                             HTTP_STATUS_401, \
                             HTTP_STATUS_409, \
                             HTTP_STATUS_500
#from debug import debug, dprint, disableDebug; disableDebug()
from skUtilities import runProgram, \
                        getEnvironmentVariables, \
                        getFilenameForCookie, \
                        getUsernameFromCookie, \
                        return401, \
                        getClientData, \
                        getClientsDataPathname, \
                        getObjectiveFromBidParameters, \
                        saveNewClientsDataFile, \
                        generateOutput, \
                        OBJECTIVES, \
                        CLIENT_TRANSLATIONS

from getDailyBudget import getDailyBudget

NOT_SET_FLOATING_VALUE = -1.0 # Sentinel value; all valid floating numbers are positive.
EMAIL_TO               = "scott.kaplan@adoya.io"
EMAIL_BCC              = ("davidschachter@gmail.com",)



# ------------------------------------------------------------------------------
#@debug
def initialize():
  definitions = {"REQUEST_METHOD": { "type" : str, "value": "POST" },
                 "CONTENT_TYPE":   { "type" : str, "value": "application/x-www-form-urlencoded" },
                 "CONTENT_LENGTH": { "type" : int, "min" : 10, "max" : 500 },
                 "HTTP_COOKIE":    { "type" : str },
                }

  return getEnvironmentVariables(definitions)
 


# ------------------------------------------------------------------------------
#@debug
def return409():
  content = "Someone else changed this while you were looking at it.  Their changes might conflict with yours so I'm not making your changes."

  return HTTP_STATUS_409, {"Content-Type"   : "text/plain",
                           "Content-Length" : f"{len(content)}",
                          }, content



# ------------------------------------------------------------------------------
#@debug
def getData(contentLength):
  data = sys.stdin.read(contentLength)

  #dprint(f"The data is:\n{data}")

  data = dict(urllib.parse.parse_qsl(data))

  requiredKeys = ("old-objective-type",   "objective-type",
                  "old-cost-per-install", "cost-per-install",
                  "old-daily-budget",     "daily-budget")

  result = { }

  # Is everything that should be present present?
  for requiredKey in requiredKeys:
    value = data.get(requiredKey)
    if value == None:
      print(f"For data {data}, key {requiredKey} was missing.", file=sys.stderr)

      return None # EARLY RETURN

    result[requiredKey] = value


  # Is everything that should be numeric numeric or where allowed, empty?
  numericRequireds = ("old-cost-per-install", "cost-per-install",
                      "old-daily-budget",     "daily-budget")

  for numericRequired in numericRequireds:
    value = result[numericRequired].strip()

    try:
      result[numericRequired] = float(value)

    except ValueError:
      print(f"For data {data}, key {numericRequired} had a non-numeric value, {value}.", file=sys.stderr)
      return None # EARLY RETURN


  # Is everything that should be one of "growth," "efficiency," "both," or nothing correct?
  objectiveTypes = ("old-objective-type", "objective-type")
  objectiveTypeValues = ("growth", "efficiency", "both", "")

  for objectiveType in objectiveTypes:
    if result[objectiveType] not in objectiveTypeValues:
      # The next line tests for old-objective-type="noneOfTheAbove" which can occur if
      # Scott Kaplan modifies the values in a non-standard way from the administration
      # console.
      if objectiveType == "objective-type" or result[objectiveType] != "noneOfTheAbove":
        print(f"For data {data}, key {objectiveType} had an incorrect value, {result[objectiveType]}.", file=sys.stderr)
        return None # EARLY RETURN


  # Everything checks out ok.
  return result



# ------------------------------------------------------------------------------
def sendEmailRegardingDailyBudget(clientData, dailyBudget, oldDailyBudget):
  clientName = clientData["clientName"]

  sys.path.append("../")
  from configuration import SMTP_HOSTNAME, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, EMAIL_FROM
  
  msg = email.message.EmailMessage()
  msg.set_content(f"""Client {clientName}
New Budget Submitted: {dailyBudget}
Old Budget: {oldDailyBudget}""")

  msg['Subject'] = f"[{clientName}] Budget Update Submission"
  msg['From']    = EMAIL_FROM
  msg['To']      = EMAIL_TO
  msg['Bcc']     = EMAIL_BCC

  with smtplib.SMTP(host=SMTP_HOSTNAME, port=SMTP_PORT) as smtpServer:
    smtpServer.set_debuglevel(2)
    smtpServer.starttls()
    smtpServer.login(SMTP_USERNAME, SMTP_PASSWORD)
    smtpServer.send_message(msg)



# ------------------------------------------------------------------------------
#@debug
def updateData(username, data, cookieMagicNumber):
  clientData, clientsArray = getClientData(username)

  bidParameters = clientData["bidParameters"]

  currentObjective = getObjectiveFromBidParameters(bidParameters)

  if currentObjective                              != data["old-objective-type"] or \
     bidParameters["HIGH_CPI_BID_DECREASE_THRESH"] != data["old-cost-per-install"] or \
     getDailyBudget(clientData)                    != data["old-daily-budget"]:
    return return409()

#   Get the new data.
  objective             = data["objective-type"]
  maximumCostPerInstall = data["cost-per-install"]
  dailyBudget           = data["daily-budget"]

#   If the user changed something, update the client record in memory accordingly.
  updateRequired = False

  if objective != data["old-objective-type"]:
    newObjectives = OBJECTIVES.get(objective)

    bidParameters.update(newObjectives)
    clientData["adgroupBidParameters"].update(newObjectives)
    updateRequired = True

  if maximumCostPerInstall != data["old-cost-per-install"]:
    bidParameters["HIGH_CPI_BID_DECREASE_THRESH"] = maximumCostPerInstall
    updateRequired = True
  
  if dailyBudget != data["old-daily-budget"]:
    sendEmailRegardingDailyBudget(clientData, dailyBudget, data["old-daily-budget"])
  
  if updateRequired == True:
    clientsArray = [clientData if client["clientName"] == username else client for client in clientsArray]

    # TODO: Check for races against other clients.  --DS, 21-Apr-19
    saveNewClientsDataFile(getClientsDataPathname(), clientsArray)
        
  return generateOutput(username, cookieMagicNumber)



# ------------------------------------------------------------------------------
#@debug
def process(initializationData):
  cookie = initializationData["HTTP_COOKIE"]
  username, cookieMagicNumber = getUsernameFromCookie(cookie)
  if username == None:
    print(f"Cookie '{cookie}' didn't map to a username.", file=sys.stderr)
    return return401()

  username = CLIENT_TRANSLATIONS.get(username, username)

  data = getData(initializationData["CONTENT_LENGTH"]);
  
  return updateData(username, data, cookieMagicNumber)



# ------------------------------------------------------------------------------
#@debug
def terminate(initializationData):
  pass
 


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  runProgram(initialize, process, terminate)
