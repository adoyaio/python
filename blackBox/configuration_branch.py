#branch api endpoint
BRANCH_ANALYTICS_URL_BASE = "https://api2.branch.io/v1/query/analytics"

#mapping of dynamo table to branch event name
data_sources = {}
data_sources['branch_commerce_events'] = 'eo_commerce_event'
data_sources['branch_opens'] = 'eo_open'
data_sources['branch_installs'] = 'eo_install'
data_sources['branch_reinstalls'] = 'eo_reinstall'


#mapping of branch event to aggregation type
aggregations = {}
aggregations['eo_commerce_event'] = ['unique_count', 'revenue']
aggregations['eo_open'] = ['unique_count']
aggregations['eo_install'] = ['unique_count']
aggregations['eo_reinstall'] = ['unique_count']
