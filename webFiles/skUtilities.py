#! /usr/bin/python3
#! MACINTOSH /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import json
import os
import re
import sys
import time
import traceback

from cgiConfiguration import DATA_DIR, \
                             CLIENT_START_PAGE_TEMPLATE, \
                             CLIENTS_DATA_FILENAME, \
                             HTTP_STATUS_OK, \
                             HTTP_STATUS_401, \
                             HTTP_STATUS_500
#from debug import debug, dprint, disableDebug; #disableDebug()
from getDailyBudget import getDailyBudget


OBJECTIVES = \
  {"growth"    : {"STALE_RAISE_BID_BOOST": 1.2,  "LOW_CPA_BID_BOOST": 1.2,  "HIGH_CPA_BID_DECREASE": 0.9},
   "efficiency": {"STALE_RAISE_BID_BOOST": 1.1,  "LOW_CPA_BID_BOOST": 1.1,  "HIGH_CPA_BID_DECREASE": 0.8},
   "both"      : {"STALE_RAISE_BID_BOOST": 1.15, "LOW_CPA_BID_BOOST": 1.15, "HIGH_CPA_BID_DECREASE": 0.85},
  }

USERNAME_TRANSLATIONS = { "rexton" : "client324",
                          "rnahm"  : "client364",
                        }

CLIENT_TRANSLATIONS   = { "client324" : "HER",
                          "client364" : "Covetly",
                        }



# ------------------------------------------------------------------------------
#@debug
def getEnvironmentVariables(definitions):
  result = { }

  for name, descriptor in definitions.items():
    item = os.environ.get(name)

    if item == None:
      print(f"Missing item {name}.", file=sys.stderr)
      return False # EARLY RETURN

    if descriptor["type"] == str:
      value = descriptor.get("value")
      if value != None and item.upper() != value.upper():
        print(f"Item {name} must have value '{value}' but was '{item}'.", file=sys.stderr)
        return False # EARLY RETURN

    elif descriptor["type"] == int:
      try:
        item = int(item)

      except:
        print(f"Failed to convert {name} to integer; text is '{item}'.", file=sys.stderr)
        return False # EARLY RETURN
 
      min, max = descriptor.get("min"), descriptor.get("max")

      if min != None and item < min:
        print(f"Item {name} has value {item} < minimum {min}.", file=sys.stderr)
        return False # EARLY RETURN

      if max != None and item > max:
        print(f"Item {name} has value {item} > maximum {max}.", file=sys.stderr)
        return False # EARLY RETURN
      

    result[name] = item

  return result



# ------------------------------------------------------------------------------
#@debug
def getFilenameForCookie(filenameFragment):
  return os.path.join(DATA_DIR, f"user_{filenameFragment}.txt")



# ------------------------------------------------------------------------------
#@debug
def return401():
  content = "Invalid username or password."

  return HTTP_STATUS_401, {"Content-Type"   : "text/plain",
                           "Content-Length" : f"{len(content)}",
                          }, content 



# ------------------------------------------------------------------------------
#@debug
def getClientsDataPathname():
  return os.path.join(DATA_DIR, CLIENTS_DATA_FILENAME)



# ------------------------------------------------------------------------------
#@debug
def getClientData(username):
  result = None

  with open(getClientsDataPathname()) as handle: 
    clientsArray = json.load(handle)

  for clientData in clientsArray: # Linear search is ok while the number of clients is small.
    if clientData["clientName"] == username and clientData["disabled"] == False:
      result = clientData
      break   

  return result, clientsArray



# ------------------------------------------------------------------------------
#@debug
def getUsernameFromCookie(cookie):
  """We expect data like this:

     who=123456789
  """

  username = None
  magicNumber = None

  if cookie.startswith("who="):
    magicNumber = cookie.split("who=", 1)[1]
    if re.match("\d+$", magicNumber): # The value should be all digits.
      with open(os.path.join(DATA_DIR, f"user_{magicNumber}.txt")) as handle:
        username = handle.read().strip()

  return username, magicNumber



# ------------------------------------------------------------------------------
#@debug
def getObjectiveFromBidParameters(bp):
  # This could be more elegantly written with an abstraction but concrete code is easy to understand.
  if   bp["STALE_RAISE_BID_BOOST"] == OBJECTIVES["growth"    ]["STALE_RAISE_BID_BOOST"] and \
       bp["LOW_CPA_BID_BOOST"    ] == OBJECTIVES["growth"    ]["LOW_CPA_BID_BOOST"    ] and \
       bp["HIGH_CPA_BID_DECREASE"] == OBJECTIVES["growth"    ]["HIGH_CPA_BID_DECREASE"]:
    result = "growth"

  elif bp["STALE_RAISE_BID_BOOST"] == OBJECTIVES["efficiency"]["STALE_RAISE_BID_BOOST"] and \
       bp["LOW_CPA_BID_BOOST"    ] == OBJECTIVES["efficiency"]["LOW_CPA_BID_BOOST"    ] and \
       bp["HIGH_CPA_BID_DECREASE"] == OBJECTIVES["efficiency"]["HIGH_CPA_BID_DECREASE"]:
    result = "efficiency"

  elif bp["STALE_RAISE_BID_BOOST"] == OBJECTIVES["both"      ]["STALE_RAISE_BID_BOOST"] and \
       bp["LOW_CPA_BID_BOOST"    ] == OBJECTIVES["both"      ]["LOW_CPA_BID_BOOST"    ] and \
       bp["HIGH_CPA_BID_DECREASE"] == OBJECTIVES["both"      ]["HIGH_CPA_BID_DECREASE"]:
    result = "both"

  else:
    result = "noneOfTheAbove"

  return result



# ------------------------------------------------------------------------------
#@debug
def saveNewClientsDataFile(pathname, data):
  # TODO: Add file locking so the rename/write process is atomic.  --DS, 21-Apr-19
  os.rename(pathname, "%s.%s.bak" % (pathname, time.time()))
  
  with open(pathname, "w") as handle:
    json.dump(data, handle)



# ------------------------------------------------------------------------------
#@debug
def generateOutput(username, whoForCookie):
  clientData, clientsArray = getClientData(username)
  bidParameters = clientData["bidParameters"]

  currentObjective = getObjectiveFromBidParameters(bidParameters)

  objectives = { "growth"         : ("checked", "",        "",        ""),
                 "efficiency"     : ("",        "checked", "",        ""),
                 "both"           : ("",        "",        "checked", ""),
                 "noneOfTheAbove" : ("",        "",        "",        "checked")
               }

  objectiveGrowth, objectiveEfficiency, objectiveBoth, objectiveNOTA = objectives[currentObjective]
    
  substitutions = { "objectiveGrowth"     : objectiveGrowth, 
                    "objectiveEfficiency" : objectiveEfficiency,
                    "objectiveBoth"       : objectiveBoth,
                    "objectiveNOTA"       : objectiveNOTA,
                    "costPerInstall"      : bidParameters["HIGH_CPI_BID_DECREASE_THRESH"],
                    "dailyBudget"         : getDailyBudget(clientData)
                  }

  with open(os.path.join(DATA_DIR, CLIENT_START_PAGE_TEMPLATE)) as handle:
    content = handle.read() % substitutions

  headers = { "Content-Type"   : "text/html",
              "Content-Length" : f"{len(content)}",
              "Set-Cookie"     : f"who={whoForCookie}; Path=/; Domain=.adoyaportal.io", # TODO: Make https only. --DS, 19-May-2019
            }
 
  return HTTP_STATUS_OK, headers, content



# ------------------------------------------------------------------------------
#@debug
def runProgram(initialize, process, terminate):
  initializationData = initialize()

  if type(initializationData) == dict:
    result = ""

    try:
      httpStatus, headers, content = process(initializationData)

      result = f"{httpStatus}\n" + \
               f"\n".join([f"{name}: {value}" for name, value in headers.items()]) + \
               f"\n\n" + \
               content

    except:
      print(f"Server failure: {traceback.format_exc()}.", file=sys.stderr)
      print(HTTP_STATUS_500 + "\n\nServer NFS failure.")

    else:
      print(result, end="")

    finally:
      terminate(initializationData)

  else: # Initialization failed.  Can't continue.
    print(HTTP_STATUS_500 + "\n\nServer LDAP failure.")
