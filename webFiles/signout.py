#! /usr/bin/python3
#! MACINTOSH /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import http.cookies
import os
import re
import sys
import traceback

from cgiConfiguration import DATA_DIR, CREDENTIALS_FILENAME, \
                             HTTP_STATUS_303, HTTP_STATUS_401, HTTP_STATUS_500, \
                             CLIENT_SIGNIN_PAGE
#from debug import debug, dprint, disableDebug#; disableDebug()

# TODO: Rewrite to use skUtilities.py, eliminating a lot of code. --DS, 19-May-2019
from skUtilities import getEnvironmentVariables


#@debug
def initialize():
  definitions = {"HTTP_COOKIE": { "type" : str },
                }

  return getEnvironmentVariables(definitions)
 


# ------------------------------------------------------------------------------
#@debug
def return401():
  content = "Not logged in."

  return HTTP_STATUS_401, {"Content-Type"   : "text/plain",
                           "Content-Length" : f"{len(content)}",
                          }, content



# ------------------------------------------------------------------------------
#@debug
def process(initializationData):
  cookies = http.cookies.SimpleCookie()
  cookies.load(initializationData["HTTP_COOKIE"])
  
  who = cookies.get("who")

  if who == None:
    print("Attempt to logout when not logged in.", file=sys.stderr)
    return return401()

  whoValue = who.value
  if re.match("^[0-9]*$", whoValue) == None:
    print(f"Attempt to logout with an invalid \"who\" cookie value, '{whoValue}'.", file=sys.stderr)
    return return401()

  try:
    os.remove(os.path.join(DATA_DIR, f"user_{whoValue}.txt"))

  except FileNotFoundError as err:
    print(f"Couldn't find file for cookie {who.value}: error is {err}.", file=sys.stderr)
    # Continue on to send the user to the front page.

  content = "".join([
    f"""<html><head><title>Adoya Client Logout Redirect</title>"""
    """<style>h1, h2 {{ font-family: sans-serif; }}</style>"""
    f"""<meta http-equiv="refresh" content="0;URL='{CLIENT_SIGNIN_PAGE}'" />"""
    f"""</head>"""
    f"""<body><h1><img src="/adoyalogo.png" border="0">You have logged out. Redirecting to"""
    f""" <a href="{CLIENT_SIGNIN_PAGE}">the main page<a> now.</h1>""",
    f"""</body></html>"""
  ])

  headers = { "Content-Type"   : "text/html",
              "Content-Length" : f"{len(content)}",
              "Location"       : f"{CLIENT_SIGNIN_PAGE}",
              "Set-Cookie"     : f"who={whoValue}; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Max-Age=0; Path=/; Domain=.adoyaportal.io.com", # TODO: Set for https only. Centralize in one place. --DS, 19-May-2019
            }
 
  return HTTP_STATUS_303, headers, content



# ------------------------------------------------------------------------------
#@debug
def terminate(initializationData):
  pass
 


# ------------------------------------------------------------------------------
#@debug
def runProgram():
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



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  runProgram()
