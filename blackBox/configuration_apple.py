APPLE_SEARCHADS_URL_BASE                = "https://api.searchads.apple.com/api/v1/"

APPLE_KEYWORDS_REPORT_URL               = APPLE_SEARCHADS_URL_BASE + "reports/campaigns"
APPLE_UPDATE_POSITIVE_KEYWORDS_URL      = APPLE_SEARCHADS_URL_BASE + "keywords/targeting"
APPLE_UPDATE_NEGATIVE_KEYWORDS_URL      = APPLE_SEARCHADS_URL_BASE + "keywords/negative"
APPLE_KEYWORD_REPORTING_URL_TEMPLATE    = APPLE_SEARCHADS_URL_BASE + "reports/campaigns/%s/keywords"
APPLE_KEYWORD_SEARCH_TERMS_URL_TEMPLATE = APPLE_SEARCHADS_URL_BASE + "reports/campaigns/%s/searchterms"
# From Search_Ads_API_July_2018.pdf, last item on p. 64: 
APPLE_ADGROUP_REPORTING_URL_TEMPLATE    = APPLE_SEARCHADS_URL_BASE + "reports/campaigns/%s/adgroups" # POST
# From Search_Ads_API_July_2018.pdf, first item on p. 41: 
APPLE_ADGROUP_UPDATE_URL_TEMPLATE       = APPLE_SEARCHADS_URL_BASE + "campaigns/%s/adgroups/%s" # PUT
