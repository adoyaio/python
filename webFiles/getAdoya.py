#! /usr/bin/python3

import json
import os
import re
import sys
import urllib.parse

# TODO: Change to use "from Clients import CLIENTS". --DS, 10-Oct-2018
from cgiConfiguration import DATA_DIR, CLIENTS_DATA_FILENAME 


threshFixer = re.compile("Thresh$").sub

content = [
  """<html><head><title>Adoya Client Bid Info</title>""",
  """<style>""",
  """h1, h2 { font-family: sans-serif; }""",
  """caption { font-weight: bold; font-size: 125%; margin-top: 15px; margin-bottom: 10px; }""",
  """.numberInput { width: 5em; }""",
  """</style>""",
  """</head>""",
  """<body><h1><img src="/adoyalogo.png" border="0">Client Parameter Control</h1><form action="/cgi-bin/putAdoya.py" method="POST">""",
]

# TODO: Change the next line to use getClientsDataPathname() from skUtilities.py. --DS, 21-Apr-19
with open(os.path.join(DATA_DIR, CLIENTS_DATA_FILENAME)) as handle:
  clients = json.load(handle)

for client in sorted(clients, key=lambda item : item["clientName"]):
  if client.get("disabled") == True:
    continue

  content.append("""<table border="1" cellspacing="1"><caption>%s</caption><thead><tr><th colspan="2">Bid Adjuster</th></tr></thead><tbody>""" % client["clientName"])

  # TODO: Refactor a lot of this logic to handle the "Adgroup" bid parameters. --DS, 24-Now-2018
  for name, value in client["bidParameters"].items():
    # TODO: Implement minimum and maximum values for each field. --DS, 23-Aug-2018
    content.append("""<tr><td>%s</td><td><input class="numberInput" type="number" name="Bid_%s_%s" value="%s" step="0.01"></td></tr""" % \
                   (threshFixer("Threshold", name.replace("_", " ").title().replace("Cpi", "CPI").replace("Cpa", "CPA")),
                    client["orgId"],
                    name,
                    value))

  content.append("""</tbody></table><br>""")

  content.append("""<table border="1" cellspacing="1"><thead><tr><th colspan="2">Keyword Adder</th></tr></thead><tbody>""")

  for name, value in client["keywordAdderParameters"].items():
    # TODO: Implement minimum and maximum values for each field. --DS, 23-Aug-2018
    content.append("""<tr><td>%s</td><td><input class="numberInput" type="number" name="KA_%s_%s" value="%s" step="0.01"></td></tr""" % \
                   (name.replace("_", " ").title(), client["orgId"], name, value))

  content.append("""</tbody></table><br>""")

content.append("""<input type="submit" name="Save" value="Save"></form></body></html>""")

result = "\n".join(content)
print("Content-Type: text/html")
print("Content-Length: %s" % len(result))
print()
print(result, end="")
