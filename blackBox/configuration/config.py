# GLOBAL
EMAIL_FROM = "info@adoya.io"
EMAIL_TO = ["james@adoya.io", "scott.kaplan@adoya.io"] #used in API only blackbox pulls from event data
TOTAL_COST_PER_INSTALL_LOOKBACK = 3
HTTP_REQUEST_TIMEOUT = 600

# APPLE
APPLE_SEARCHADS_URL_BASE_V3 = "https://api.searchads.apple.com/api/v3/"
APPLE_SEARCHADS_URL_BASE_V4 = "https://api.searchads.apple.com/api/v4/"
APPLE_KEYWORDS_REPORT_URL = "reports/campaigns"
APPLE_UPDATE_POSITIVE_KEYWORDS_URL = "campaigns/%s/adgroups/%s/targetingkeywords/bulk"
APPLE_UPDATE_NEGATIVE_KEYWORDS_URL = "campaigns/%s/adgroups/%s/negativekeywords/bulk"
APPLE_KEYWORD_REPORTING_URL_TEMPLATE = "reports/campaigns/%s/keywords"
APPLE_KEYWORD_SEARCH_TERMS_URL_TEMPLATE = "reports/campaigns/%s/searchterms"
APPLE_ADGROUP_REPORTING_URL_TEMPLATE = "reports/campaigns/%s/adgroups" # POST
APPLE_ADGROUP_UPDATE_URL_TEMPLATE = "campaigns/%s/adgroups/%s" # PUT
APPLE_CAMPAIGN_UPDATE_URL_TEMPLATE = "campaigns/%s" # PUT

APPLE_GET_APPS_URL = "search/apps?returnOwnedApps=true&limit=100"

# Auth
APPLE_AUTH_URL = "https://appleid.apple.com/auth/oauth2/token"

# BRANCH
BRANCH_ANALYTICS_URL_BASE = "https://api2.branch.io/v1/query/analytics"

# STRIPE
STRIPE_API_KEY = "sk_test_51I7PknCY7kJiUoJdxbiY3H8233hPnmArovZaVwyTkv4YeGN4lUaIADNmt3SwjW0VeqG0y74Wa7khT9Alqix2ebVE00PTMt2g21"


# table to branch event
DATA_SOURCES = {}
DATA_SOURCES['branch_commerce_events'] = 'eo_commerce_event'
DATA_SOURCES['branch_opens'] = 'eo_open'
DATA_SOURCES['branch_installs'] = 'eo_install'
DATA_SOURCES['branch_reinstalls'] = 'eo_reinstall'

# branch event to aggregation type
AGGREGATIONS = {}
AGGREGATIONS['eo_commerce_event'] = ['unique_count', 'revenue']
AGGREGATIONS['eo_open'] = ['unique_count']
AGGREGATIONS['eo_install'] = ['unique_count']
AGGREGATIONS['eo_reinstall'] = ['unique_count']
