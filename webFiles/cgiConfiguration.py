CERTIFICATES_DIR      = "/home/scott/ScottKaplan/Certificates"
DATA_DIR              = "../Data"
CLIENTS_DATA_FILENAME = "clients.json"
CREDENTIALS_FILENAME  = "credentials.json"

DAILY_BUDGET_URL = """https://api.searchads.apple.com/api/v1/campaigns/%s?""" + \
                   """fields=id,name,dailyBudgetAmount,servingStatus"""

# The Python CGIHttpServer sends a 200 OK before we get a chance so none of these are useful:
HTTP_STATUS_OK    = "200 Ok"
HTTP_STATUS_303   = "303 See Other"
HTTP_STATUS_401   = "401 Unauthorized"
HTTP_STATUS_409   = "409 Conflict"
HTTP_STATUS_500   = "500 Internal Server Error"
HTTP_REQUEST_TIMEOUT = 20 # seconds (duplicated in ../../configuration.py)

CLIENT_SIGNIN_PAGE = "/index.html"
CLIENT_START_PAGE_TEMPLATE  = "portal.html.template"
