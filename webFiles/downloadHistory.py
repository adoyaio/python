#! /usr/bin/python3
#! MACINTOSH /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import json
import os
import re
import sys
import time
import traceback
import urllib.parse

from cgiConfiguration import DATA_DIR, \
                             HTTP_STATUS_OK, HTTP_STATUS_303, HTTP_STATUS_401, HTTP_STATUS_500
def debug(func): return func; #sys.path.append(".."); from debug import debug, dprint, disableDebug; #disableDebug()
from skUtilities import runProgram, \
                        getEnvironmentVariables, \
                        getUsernameFromCookie, \
                        return401, \
                        getClientData, \
                        CLIENT_TRANSLATIONS

CLIENT_HISTORY_FILENAME_TEMPLATE = "history_%s.csv" # TODO: Centralize back in Client.py. --DS, 21-Apr-19



# ------------------------------------------------------------------------------
@debug
def initialize():
  definitions = {"REQUEST_METHOD": { "type" : str, "value": "GET" },
                 "HTTP_COOKIE":    { "type" : str },
                }

  return getEnvironmentVariables(definitions)
 


# ------------------------------------------------------------------------------
@debug
def generateCSVFile(username):
  clientData, clientsArray = getClientData(username)

  historyPathname = os.path.join(DATA_DIR, CLIENT_HISTORY_FILENAME_TEMPLATE % clientData["orgId"])

  with open(historyPathname) as handle:
    content = handle.read()

  suggestedFilename = "Adoya_%s.csv" % time.strftime("%Y-%m-%d-%H-%M-%S")

  headers = { "Content-Type"        : "text/csv",
              "Content-Length"      : f"{len(content)}",
              "Content-Disposition" : f'attachment; filename="{suggestedFilename}"',
            }
    
  return HTTP_STATUS_OK, headers, content



# ------------------------------------------------------------------------------
@debug
def process(initializationData):
  cookie = initializationData["HTTP_COOKIE"]
  username, cookieMagicNumber = getUsernameFromCookie(cookie)
  if username == None:
    print(f"Cookie '{cookie}' didn't map to a username.", file=sys.stderr)
    return return401() # EARLY RETURN

  return generateCSVFile(CLIENT_TRANSLATIONS.get(username, username))



# ------------------------------------------------------------------------------
@debug
def terminate(initializationData):
  pass
 


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  runProgram(initialize, process, terminate)
