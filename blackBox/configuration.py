# SMTP_HOSTNAME = "example.com"
# SMTP_PORT     =  "25"
# SMTP_USERNAME = "exampleName"
# SMTP_PASSWORD = "examplePassword"
# SMTP_HOSTNAME = "smtp-relay.gmail.com"
# SMTP_PORT     =  "25"
# SMTP_USERNAME = "scott.kaplan@adoya.io"
# SMTP_PASSWORD = ""
EMAIL_FROM    = "info@adoya.io"
TOTAL_COST_PER_INSTALL_LOOKBACK = 7
HTTP_REQUEST_TIMEOUT = 100

from configuration_apple import APPLE_KEYWORDS_REPORT_URL, \
                                APPLE_UPDATE_POSITIVE_KEYWORDS_URL, \
                                APPLE_UPDATE_NEGATIVE_KEYWORDS_URL, \
                                APPLE_KEYWORD_REPORTING_URL_TEMPLATE, \
                                APPLE_KEYWORD_SEARCH_TERMS_URL_TEMPLATE, \
                                APPLE_ADGROUP_REPORTING_URL_TEMPLATE, \
                                APPLE_ADGROUP_UPDATE_URL_TEMPLATE

from configuration_branch import BRANCH_ANALYTICS_URL_BASE, \
                                 data_sources, \
                                 aggregations