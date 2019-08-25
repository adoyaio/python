#! /usr/bin/python3
#! MACINTOSH /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import base64
import hashlib
import json
import os
from random import SystemRandom
import re
import sys
import time
import traceback
import urllib.parse

randint = SystemRandom().randint

from cgiConfiguration import DATA_DIR, CREDENTIALS_FILENAME, \
                             HTTP_STATUS_OK, HTTP_STATUS_303, HTTP_STATUS_401, HTTP_STATUS_500
#from debug import debug, dprint, disableDebug; disableDebug()

from skUtilities import runProgram, \
                        getEnvironmentVariables, \
                        getFilenameForCookie, \
                        return401, \
                        getClientData, \
                        getObjectiveFromBidParameters, \
                        generateOutput, \
                        USERNAME_TRANSLATIONS, \
                        CLIENT_TRANSLATIONS



# ------------------------------------------------------------------------------
#@debug
def initialize():
  definitions = {"REQUEST_METHOD": { "type" : str, "value": "POST" },
                 "CONTENT_TYPE":   { "type" : str, "value": "application/x-www-form-urlencoded"},
                 "CONTENT_LENGTH": { "type" : int, "min" : 10, "max" : 100},
                }

  return getEnvironmentVariables(definitions)
 


# ------------------------------------------------------------------------------
#@debug
def getCredentialsFromUser(contentLength):
  """We expect data like this:

     username=foo&password=bar
  """

  data = sys.stdin.read(contentLength)

  #dprint(f"The data is:\n{data}")

  credentials = dict(urllib.parse.parse_qsl(data))

  username, password = credentials.get("username"), credentials.get("password")

  if username == None or password == None:
    print(f"User-supplied credentials, {credentials}, are incomplete.", file=sys.stderr)
    return None, None # EARLY RETURN
 
  del credentials["username"]
  del credentials["password"]

  if len(credentials) > 0:
    print(f"Data in credentials after removing expected items: {credentials}.", file=sys.stderr)

  
  translation = USERNAME_TRANSLATIONS.get(username.lower().strip(), username)

  return translation, password.strip()



# ------------------------------------------------------------------------------
#@debug
def createCookieAndFile(username):
  filenameFragment = f"{randint(1, sys.maxsize)}"

  with open(getFilenameForCookie(filenameFragment), "w") as handle:
    handle.write(username)

  return filenameFragment



# ------------------------------------------------------------------------------
#@debug
def process(initializationData):
  username, password = getCredentialsFromUser(initializationData["CONTENT_LENGTH"])

  if username == None or password == None:
    print(f"User credentials are missing the username and/or password.", file=sys.stderr)
    return return401()

  password = password.replace("-", "")
  if re.match("^[0-9]*$", password) == None:
    print(f"User {username} attemped an invalid password (not echoed).", file=sys.stderr)
    return return401()
   
  with open(os.path.join(DATA_DIR, CREDENTIALS_FILENAME)) as handle:
    storedCredentials = json.load(handle)

  userCredentials = storedCredentials.get(username)

  if userCredentials == None:
    print(f"User {username} doesn't exist.", file=sys.stderr)
    return return401()

  salt, hashedPassword = userCredentials["salt"], userCredentials["hashedPassword"]

  hasher = hashlib.sha256()

  hasher.update(bytes(str(salt), "ASCII"))
  hasher.update(bytes(password,  "ASCII"))
  testValue = base64.b64encode(hasher.digest()).decode("ASCII")

  if testValue != hashedPassword:
    print(f"User {username} entered an invalid password (not echoed).", file=sys.stderr)
    return return401()

  return generateOutput(CLIENT_TRANSLATIONS.get(username, username), createCookieAndFile(username))



# ------------------------------------------------------------------------------
#@debug
def terminate(initializationData):
  pass
 


# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  runProgram(initialize, process, terminate)
