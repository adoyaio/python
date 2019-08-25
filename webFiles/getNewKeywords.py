#! /usr/bin/python3
#! MACINTOSH /Library/Frameworks/Python.framework/Versions/3.7/bin/python3

import json
import os
import sys

from cgiConfiguration import DATA_DIR, CLIENTS_DATA_FILENAME


content = [
  """<html><head><title>Adoya Keyword Adder</title>""",
  """<style>""",
  """h1, h2 { font-family: sans-serif; }""",
  """.clientName { font-weight: bold; font-size: 125%; margin-top: 15px; margin-bottom: 10px; }""",
  """</style>""",
  """</head>""",
  """<body><h1><img src="/adoyalogo.png" border="0">Keyword Adder</h1><form action="/cgi-bin/putNewKeywords.py" method="POST">""",
]

with open(os.path.join(DATA_DIR, CLIENTS_DATA_FILENAME)) as h:
  clients = json.load(h)

for client in sorted(clients, key=lambda item : item["clientName"]):
  if client.get("disabled") == True:
    continue

  content.append("""<div class="clientName">%(clientName)s</div>
<textarea name="%(orgId)s" rows="4" cols="50" placeholder="One keyword per line, please."></textarea>
<br>""" % client)

content.append("""<br><input type="submit" name="Save" value="Save"></form></body></html>""")

result = "\n".join(content)
print("Content-Type: text/html")
print("Content-Length: %s" % len(result))
print()
print(result, end="")
