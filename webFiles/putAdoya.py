#! /usr/bin/python3

import json
import os
import sys
import time
import urllib.parse

# TODO: Change to use "from Clients import CLIENTS". --DS, 10-Oct-2018
from cgiConfiguration import DATA_DIR, CLIENTS_DATA_FILENAME

# TODO: Add password checking and https encryption. --DS, 23-Aug-2018

# TODO: Change the next line to use getClientsDataPathname() from skUtilities.py. --DS, 21-Apr-19
CLIENTS_DATA_PATHNAME = os.path.join(DATA_DIR, CLIENTS_DATA_FILENAME)

with open(CLIENTS_DATA_PATHNAME) as h:
  clientsArray = json.load(h)

clients = { client["orgId"] : client for client in clientsArray }

webData = urllib.parse.parse_qs(sys.stdin.read(int(os.environ["CONTENT_LENGTH"])))

for key, value in webData.items():
  if key != "Save":
    # TODO: Refactor a lot of this logic to handle "Adgroup" bid parameters. --DS, 24-Nov-2018
    destination, orgId, parameterName = key.split("_", 2)

    orgIdInt = int(orgId)

    if destination == "Bid":
      clients[orgIdInt]["bidParameters"][parameterName] = float(value[-1])

    elif destination == "KA":
      clients[orgIdInt]["keywordAdderParameters"][parameterName] = float(value[-1])

newClientsArray = [value for value in clients.values()]

# TODO: Change the few lines below to use saveNewClientsDataFile() in skUtilities.py. --DS, 21-Apr-2019
os.rename(CLIENTS_DATA_PATHNAME, "%s.%s.bak" % (CLIENTS_DATA_PATHNAME, time.time()))

with open(CLIENTS_DATA_PATHNAME, "w") as h:
  json.dump(newClientsArray, h)


content = [
  """<html><head><title>Adoya Client Bid Info</title>""",
  """<style>h1, h2 { font-family: sans-serif; }</style>"""
  """</head>""",
  """<body><h1><img src="/adoyalogo.png" border="0">Client Parameter Control</h1>""",
  """<h1>Data Updated</h1><a href="/cgi-bin/getAdoya.py">Continue</a></body></html>""",
]

result = "\n".join(content)
print("Content-Type: text/html")
print("Content-Length: %s" % len(result))
print()
print(result, end="")
