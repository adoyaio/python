import decimal
import boto3
import json
import time
import datetime
from utils.debug import debug, dprint
from utils import DynamoUtils, EmailUtils, LambdaUtils
from configuration import config
from utils.DecimalEncoder import DecimalEncoder
from Client import Client
import requests
from requests.adapters import HTTPAdapter, Retry
import urllib3


def patchAppleCampaign(event, context):
    print('Loading postAppleCampaign....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received identity: " + str(context.identity))

    updateCampaignData: list = json.loads(event["body"])
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    table = dynamodb.Table('clients_2')
    client: Client = DynamoUtils.getClient(dynamodb, org_id)

    if client.auth is None:
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': 'x-api-key, Authorization'
            },
            'body': {}
        }

    # print(str(updateCampaignData))

    # go thru each campaign in the payload and update 
    existingCampaigns = client.appleCampaigns

    print("found auth values in client " + str(client.auth))
    authToken = LambdaUtils.getAuthToken(client.auth, client.orgId)

    for newCampaignValues in updateCampaignData:
        
        # build the new campaigns for 
        adoyaCampaign = next(filter(lambda x: x['campaignId'] == newCampaignValues['campaignId'], existingCampaigns), None)
        if adoyaCampaign is not None:
            adoyaCampaign['status'] = newCampaignValues['status']
            adoyaCampaign['lifetimeBudget'] = newCampaignValues['lifetimeBudget']
            adoyaCampaign['dailyBudget'] = newCampaignValues['dailyBudget']


        url = config.APPLE_SEARCHADS_URL_BASE_V4 + (config.APPLE_CAMPAIGN_UPDATE_URL_TEMPLATE % newCampaignValues['campaignId'])
        headers = {
            "Authorization": "Bearer %s" % authToken, 
            "X-AP-Context": "orgId=%s" % client.orgId,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        payload = {
            "campaign": {
                 "budgetAmount": {
                    "amount": str(newCampaignValues['lifetimeBudget']),
                    "currency": str(client.currency)
                },
                "dailyBudgetAmount": {
                    "amount": str(newCampaignValues['dailyBudget']),
                    "currency": str(client.currency)
                },
                "status": str(newCampaignValues['status']),
            }
        }
        print("Apple URL is" + url)
        print("Headers are" + str(headers))
        print("Payload is '%s'." % payload)
        response = requests.put(
            url,
            json=payload,
            headers=headers,
            timeout=config.HTTP_REQUEST_TIMEOUT
        )
        print(str(response.text))
        print("The result of PUT campaign to Apple: %s" % response)

   
    # write to db
    updated = json.loads(client.toJSON(), parse_float=decimal.Decimal)

    # TODO test this
    updated['orgId'] = updated.get('asaId')
    updated.pop('asaId') # in clients json there is no asaId key, orgId on orgdetail represents asaid
    
    table.put_item(
        Item = {
                'orgId': org_id,
                'orgDetails': updated
            }
    )

    # table.put_item(
    #     Item = updated
    # )

    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': 'x-api-key, Authorization'
        },
        'body': response.text
    }


def postAppleCampaign(event, context):
    print('Loading postAppleCampaign....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received identity: " + str(context.identity))

    # environment details
    if LambdaUtils.getApiEnvironmentDetails(event).get('send'):
        campaignStatus = "ENABLED"
    else:
        campaignStatus = "PAUSED"

    # queryStringParameters = event["queryStringParameters"]
    # org_id = queryStringParameters["org_id"]
    
    campaignData: dict = json.loads(event["body"])
    campaignType: str = campaignData.get('campaignType')
    authToken = campaignData.get('authToken')

    print("---------------------------------------------------")
    print("Processing createCampaign for campaignType " +  campaignType)
    print("---------------------------------------------------")


    # create competitor campaign
    campaign: any | bool = createCampaign(campaignType, campaignData, campaignStatus, authToken)
    if not campaign: 
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'x-api-key'
            },
            'body': {}
        }
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': 'x-api-key, Authorization'
        },
        'body': json.dumps(
            {'campaign': campaign }
        )
    }

def createCampaign(campaignType, campaignData, campaignStatus, authToken):
    # parse event data
    org_id = campaignData["org_id"]
    app_name = campaignData["app_name"]
    adam_id = campaignData["adam_id"]

    lifetime_budget: float = 0.0000
    daily_budget_amount: float = 0.0000

    # campaign level 
    campaign_target_country = campaignData["campaign_target_country"]
    front_end_lifetime_budget = float(campaignData["lifetime_budget"])
    front_end_daily_budget = float(campaignData["daily_budget"])
    target_cost_per_install= float(campaignData["target_cost_per_install"])
    gender=campaignData["gender"]
    min_age=campaignData["min_age"]
    targeted_keywords_competitor=campaignData["targeted_keywords_competitor"]
    targeted_keywords_category=campaignData["targeted_keywords_category"]
    targeted_keywords_brand=campaignData["targeted_keywords_brand"]
    currency=campaignData["currency"]

    print("---------------------------------------------------")
    print("Processing createCampaign for campaignType " +  campaignType)
    print("---------------------------------------------------")

    # pivot on campaign type to readable campaign type
    if campaignType == 'competitor':
        campaign_type = 'Competitor - Exact Match'
    elif campaignType == 'category':
        campaign_type = 'Category - Exact Match'
    elif campaignType == 'brand':
        campaign_type = 'Brand - Exact Match'
    elif campaignType == 'exact_discovery':
        campaign_type = 'Discovery - Exact Match'
    elif campaignType == 'broad_discovery':
        campaign_type = 'Discovery - Broad Match'
    elif campaignType == 'search_discovery':
        campaign_type = 'Discovery - Search Match'
    else:
        print("Invalid campaignType")
        return False

    # set date and time to create campaign and ad group names
    now = datetime.datetime.now()

    # name campaign and providing date and time prevents duplicative naming
    campaign_name = app_name + ' - ' + campaign_target_country + ' - ' + campaign_type + ' - ' + str(now.strftime("%Y-%m-%d %H:%M:%S"))

    # budget; pivot on campaign type
    if campaignType == 'competitor':
        lifetime_budget: float = front_end_lifetime_budget * 0.30
        daily_budget_amount: float = front_end_daily_budget * 0.30
    elif campaignType == 'category':
        lifetime_budget: float = front_end_lifetime_budget * .30
        daily_budget_amount:float = front_end_daily_budget * .30
    elif campaignType == 'brand':
        lifetime_budget:float = front_end_lifetime_budget * .15
        daily_budget_amount:float = front_end_daily_budget * .15
    elif campaignType == 'exact_discovery':
        lifetime_budget:float = front_end_lifetime_budget * .05
        daily_budget_amount:float = front_end_daily_budget * .05
        print("exact_discovery")
        print(lifetime_budget)
        print(daily_budget_amount)
    elif campaignType == 'broad_discovery':
        lifetime_budget:float = front_end_lifetime_budget * .10
        daily_budget_amount:float = front_end_daily_budget * .10
    elif campaignType == 'search_discovery':
        lifetime_budget:float = front_end_lifetime_budget * 0.10
        daily_budget_amount:float = front_end_daily_budget * 0.10
    else:
        print("Invalid campaignType")
        return False

    # name your ad group
    ad_group_name = campaign_name

    # search match; pivot on campaign type
    if campaignType == 'search_discovery':
        search_match = True
    else:
        search_match = False

    # create null variable and assign it None as python doesn't have the concept of null
    null = None

    # 1. targeted keyword creation

    # relevant keywords; pivot on campaign type
    if campaignType == 'competitor':
        targeted_keywords = [each_string.lower() for each_string in targeted_keywords_competitor]
    elif campaignType == 'category':
        targeted_keywords = [each_string.lower() for each_string in targeted_keywords_category]
    elif campaignType == 'brand':
        targeted_keywords = [each_string.lower() for each_string in targeted_keywords_brand]
    elif campaignType == 'exact_discovery':
        targeted_keywords = []
    elif campaignType:
        targeted_keywords = []
        targeted_keywords.extend(
            [each_string.lower() for each_string in targeted_keywords_competitor]
        )
        targeted_keywords.extend(
            [each_string.lower() for each_string in targeted_keywords_category if each_string not in targeted_keywords]
        )
        targeted_keywords.extend(
            [each_string.lower() for each_string in targeted_keywords_brand if each_string not in targeted_keywords]
        )

    
    # enter keyword match type 
    if campaignType == 'broad_discovery':
        targeted_keyword_match_type = 'BROAD'
    else:
        targeted_keyword_match_type = 'EXACT'


    # negative kw match type, always EXACT
    negative_ad_group_match_type = 'EXACT'

    # set current time as ad group start date
    ad_group_start_date_time = str(datetime.datetime.now()).replace(' ','T')

    headers = {
        "Authorization": "Bearer %s" % authToken, 
        "X-AP-Context": "orgId=%s" % org_id,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    base_url_4 = config.APPLE_SEARCHADS_URL_BASE_V4
    # base_url = config.APPLE_SEARCHADS_URL_BASE_V3


    # daily cap may not exceed total budget
    if lifetime_budget > daily_budget_amount:
         print('Lifetime Budget Valid')
    else:
        print("Invalid Lifetime Budget & Daily Cap Entry.")
        print(lifetime_budget)
        print(daily_budget_amount)
        return False

    # target CPI must not exceed daily budget cap
    # NOTE 20x front_end_daily_budget should be 20x CPI, therefore .05 (for exact_discovery) * front_end_daily_budget should be >= CPI
    # NOTE consider updating validation check to match f/e check  
    if daily_budget_amount >= target_cost_per_install:
        print('Daily Budget Valid')
    else:
        print("Invalid Daily Cap & Max CPI Entry")
        print(daily_budget_amount)
        print(target_cost_per_install)
        return False

    # logic to determine gender targeting this is done at the ad group level
    if gender == 'male':
        gender = ['M']
    elif gender == 'female':
        gender = ['F']
    elif gender == 'all':
        gender = ['M','F']
    else:
        print("Invalid Gender Targeting Entry.")
        return False

    # logic to determine min age targeting this is done at the ad group level
    if min_age == "18":
        min_age = 18
    elif min_age == "21":
        min_age = 21
    elif min_age == 'all':
        min_age = null
    else:
        print("Invalid Gender Targeting Entry.")
        return False

    # logic to ensure keywords are entered for all but exact_discovery
    if (len(targeted_keywords) == 0) & (campaignType != 'exact_discovery'):
        print('Keywords required for all campaigns other than exact_discovery')
        return False


    # create a new campaign
    create_campaign_payload = {
        "orgId": org_id,
        "adChannelType": "SEARCH",
        "supplySources": ["APPSTORE_SEARCH_RESULTS"],    
        "name": str(campaign_name),
        "budgetAmount": {
          "amount": str(lifetime_budget),
          "currency": str(currency)
        },
        "dailyBudgetAmount": {
          "amount": str(daily_budget_amount),
          "currency": str(currency)
        },
        "adamId": adam_id,
        "countriesOrRegions": [str(campaign_target_country)],
        "status": campaignStatus, # ENABLED or PAUSED, set per env via LambdaUtils  
        "billingEvent": "TAPS"
    }

    
    
    # create_campaign_url = base_url + "campaigns"
    create_campaign_url = base_url_4 + "campaigns"

    print("Payload is '%s'." % create_campaign_payload)
    print("Url is '%s'." % create_campaign_url)

    # HTTP retry implementation
    http = urllib3.PoolManager()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504 ])
    
    encoded_data = json.dumps(create_campaign_payload).encode('utf-8')
    create_campaign_response = http.request('POST', create_campaign_url, body=encoded_data, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT, retries=retries)

    # create_campaign_response = requests.post(
    #     create_campaign_url,  
    #     json=create_campaign_payload,
    #     headers=headers
    # )

    # print ("The result of POST campaign to Apple: %s" % create_campaign_response)
    print ("The result of POST campaign to Apple: %s" % create_campaign_response.data)

    # error handling
    if create_campaign_response.status != 200:
        return False

    # 2. create ad group

    # get the most recent campaign id
    get_campaigns_payload = {
        "fields":[
            "id","name",
             "adamId","budgetAmount",
            "dailyBudgetAmount",
            "status","servingStatus"
        ],
        "conditions":[
            {
                "field":"servingStatus",
                "operator":"IN",
                "values":["NOT_RUNNING"]
            }
        ],
        "orderBy":[
            {
                "field":"id",
                "sortOrder":"DESCENDING"
            }
        ],
        "pagination":{"offset":0,"limit":1000}
    }

    # get_campaigns_url = base_url + "campaigns/find"
    get_campaigns_url = base_url_4 + "campaigns/find"

    print ("url is '%s'." % get_campaigns_url)
    print ("Payload is '%s'." % get_campaigns_payload)


    encoded_data = json.dumps(get_campaigns_payload).encode('utf-8')
    get_campaigns_response = http.request('POST', get_campaigns_url, body=encoded_data, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT, retries=retries)

    # get_campaigns_response = requests.post(
    #     get_campaigns_url,
    #     json=get_campaigns_payload,
    #     headers=headers
    # ) 

    print ("The result of get campaigns from apple : %s" % get_campaigns_response.data)

    # extract all the apps assignd to the apple search ads account
    # campaign_id_data = json.loads(get_campaigns_response.text) 
    campaign_id_data = json.loads(get_campaigns_response.data) 
    campaign_id_list = [campaign_id_data[x] for x in campaign_id_data]
    all_campaigns_list = campaign_id_list[0][0:1000]

    # delay script briefly so apple api has enough time to update id in their system so our script can reference it
    time.sleep(5)

    # get the adam id which then begins the campaign creation process
    new_campaign_id = [aDict['id'] for aDict in all_campaigns_list if aDict['name'] == campaign_name][0]

    # create an ad group in the new campaign
    create_ad_group_payload = {
            "name": str(ad_group_name),
            "startTime": ad_group_start_date_time,
            "automatedKeywordsOptIn": search_match,
            "defaultBidAmount": {
                "amount": str(round(target_cost_per_install * 0.50,2)),
                "currency": str(currency)
            },
            "pricingModel": "CPC",
            "targetingDimensions": {
                "age": {
                    "included": [
                        {
                            "minAge": min_age,
                            "maxAge": null
                        }
                    ]
                },
                "gender": {
                    "included": gender
                },
                "country": null,
                "adminArea": null,
                "locality": null,
                "deviceClass": null,
                "daypart": null,
                "appDownloaders": null
            }
    }

    # create_ad_group_url = base_url + "campaigns/%s/adgroups/" % new_campaign_id
    create_ad_group_url = base_url_4 + "campaigns/%s/adgroups/" % new_campaign_id


    print ("Payload is '%s'." % create_ad_group_payload)
    print ("Url is '%s'." % create_ad_group_url)

    encoded_data = json.dumps(create_ad_group_payload).encode('utf-8')
    create_ad_group_response = http.request('POST', create_ad_group_url, body=encoded_data, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT, retries=retries)

    # create_ad_group_response = requests.post(
    #     create_ad_group_url,
    #     json=create_ad_group_payload,
    #     headers=headers
    # ) 

    print ("The result of POST adgroups to Apple: %s" % create_ad_group_response.data)

    # error handling
    if create_ad_group_response.status != 200:
        return False

    # 3. create targeted keywords

    # get the most recent ad group id
    get_adgroups_payload = {
        "fields":["id","name"],
        "conditions":[
            {
                "field":"servingStatus",
                "operator":"IN",
                "values":["NOT_RUNNING","RUNNING"]
            }
        ],
        "orderBy":[
            {
                "field":"id",
                "sortOrder":"DESCENDING"
            }
        ],
        "pagination":
            {"offset":0,"limit":1000}
    }

    # get_adgroups_url = base_url + "campaigns/%s/adgroups/find" % new_campaign_id
    get_adgroups_url = base_url_4 + "campaigns/%s/adgroups/find" % new_campaign_id

    print ("Url is '%s'." % get_adgroups_url)
    print ("Payload is '%s'." % get_adgroups_payload)

    encoded_data = json.dumps(get_adgroups_payload).encode('utf-8')
    get_adgroups_response = http.request('POST', get_adgroups_url, body=encoded_data, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT, retries=retries)

    # get_adgroups_response = requests.post(
    #     base_url + "campaigns/%s/adgroups/find" % new_campaign_id,
    #     json=get_adgroups_payload,
    #     headers=headers
    # ) 

    print ("The result of getting adgroups from Apple: %s" % get_adgroups_response.data)

    # extract all the ad groups
    ad_group_data = json.loads(get_adgroups_response.data) 
    ad_group_data_list = [ad_group_data[x] for x in ad_group_data]
    all_ad_groups_list = ad_group_data_list[0][0:1000]

    # delay script briefly so apple api has enough time to update id in their system so our script can reference it
    time.sleep(5)

    #get the ad group which begins the keyword adding process
    new_ad_group_id = [aDict['id'] for aDict in all_ad_groups_list if aDict['name'] == ad_group_name][0]

    print("Created adgroup " + str(new_ad_group_id))

    # create NEGATIVE keywords, only broad and search
    if campaignType == 'broad_discovery' or campaignType == 'search_discovery':
        negative_keyword_payload = [
            {
                "text": item, 
                "matchType": negative_ad_group_match_type
            } 
            for item in targeted_keywords
        ]
        negative_keyword_url = base_url_4 + "campaigns/%s/adgroups/%s/negativekeywords/bulk" % (new_campaign_id, new_ad_group_id)
        print ("Url is '%s'." % negative_keyword_url)
        print ("Payload is '%s'." % negative_keyword_payload)

        encoded_data = json.dumps(negative_keyword_payload).encode('utf-8')
        create_negative_keyword_response = http.request('POST', negative_keyword_url, body=encoded_data, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT, retries=retries)

        # create_negative_keyword_response = requests.post(
        #     negative_keyword_url,
        #     json=negative_keyword_payload,
        #     headers=headers
        # )

        print("Headers are" + str(headers))
        print("Response headers" + str(create_negative_keyword_response.headers))
        print("Response is" + str(create_negative_keyword_response.data))
        # print("Response text is" + str(create_negative_keyword_response.reason))
        print("The result of posting NEGATIVE keywords to Apple: %s" % create_negative_keyword_response.status)

        # error handling
        if create_negative_keyword_response.status != 200:
           return False

    
    # create TARGETED keywords, all campaigns OTHER than search and exact
    if campaignType != 'search_discovery' and campaignType != 'exact_discovery':
        targeted_keyword_payload = [
            {
                "text": item, 
                "matchType": targeted_keyword_match_type,
                "bidAmount": 
                    {
                        "currency": str(currency), 
                        "amount": str(round(target_cost_per_install * 0.50,2))
                    }
            } 
            for item in targeted_keywords
        ]
        
        # targeted_keyword_url = base_url + "campaigns/%s/adgroups/%s/targetingkeywords/bulk" % (new_campaign_id, new_ad_group_id)
        targeted_keyword_url = base_url_4 + "campaigns/%s/adgroups/%s/targetingkeywords/bulk" % (new_campaign_id, new_ad_group_id)

        print ("Url is '%s'." % targeted_keyword_url)
        print ("Payload is '%s'." % targeted_keyword_payload)

        encoded_data = json.dumps(targeted_keyword_payload).encode('utf-8')
        create_targeted_keyword_response = http.request('POST', targeted_keyword_url, body=encoded_data, headers=headers, timeout=config.HTTP_REQUEST_TIMEOUT, retries=retries)

        # create_targeted_keyword_response = requests.post(
        #     targeted_keyword_url,
        #     json=targeted_keyword_payload,
        #     headers=headers
        # )

        print ("The result of posting keywords to Apple: %s" % create_targeted_keyword_response.data)

        # error handling
        if create_targeted_keyword_response.status != 200:
            return False

    # common campaign values
    returnVal = {
        "adGroupId": new_ad_group_id,
        "adGroupName": ad_group_name,
        "campaignId": new_campaign_id,
        "campaignName": campaign_name,
        "campaignType": campaignType,
        "lifetimeBudget": lifetime_budget,
		"dailyBudget": daily_budget_amount,
		"gender": gender,
		"minAge": min_age,
        "bidParameters": {},
        "branchBidParameters": {},
        "status": campaignStatus
    }

    # handle return logic by campaign type
    if campaignType == 'search_discovery':
        returnVal["keywordIntegrationEnabled"] = False
        returnVal["bidAdjusterEnabled"] = False
        returnVal["adgroupBidAdjusterEnabled"] = True
    else:
        returnVal["keywordIntegrationEnabled"] = True
        returnVal["bidAdjusterEnabled"] = True
        returnVal["adgroupBidAdjusterEnabled"] = False

    return returnVal

def getAppleApps(event, context):
    print('Loading getAppleApps....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    client : Client = DynamoUtils.getClient(dynamodb, org_id)

    # handle auth token
    if client.auth is not None:
        print("found auth values in client " + str(client.auth))
        authToken = LambdaUtils.getAuthToken(client.auth, client.asaId)
        

        # get apps
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_GET_APPS_URL
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % client.asaId}
        print("URL is" + url)
        print("Headers are" + str(headers))
        response = requests.get(
            url,
            headers=headers,
            timeout=config.HTTP_REQUEST_TIMEOUT
        )
        print(str(response.text))

        # get acls
        get_acls_response = requests.get(
            config.APPLE_SEARCHADS_URL_BASE_V4 + "acls",
                headers=headers
            )

        #extract all the apps assignd to the apple search ads account
        get_acls_all_orgs_response = json.loads(get_acls_response.text)

        print(str(get_acls_all_orgs_response))
        get_acls_all_orgs_list = [get_acls_all_orgs_response[x] for x in get_acls_all_orgs_response]
        get_acls_all_orgs_list_extracted = get_acls_all_orgs_list[0][0:1000]
        acls_response = list(
            filter(
                lambda org:(org["orgId"] == int(client.asaId)), get_acls_all_orgs_list_extracted
            )
        )

        
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': 'x-api-key, Authorization'
        },
        'body': json.dumps({ 'apps' : json.loads(response.text), 'acls': acls_response })
    }

def getAppleCampaigns(event, context):
    print('Loading getAppleCampaigns....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    client : Client = DynamoUtils.getClient(dynamodb, org_id)

    # handle auth token
    if client.auth is not None:
        print("found auth values in client " + str(client.auth))
        authToken = LambdaUtils.getAuthToken(client.auth, client.asaId)
        
        # get campaigns
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_CAMPAIGNS_URL
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % client.asaId}
        print("URL is" + url)
        print("Headers are" + str(headers))
        response = requests.get(
            url,
            headers=headers,
            timeout=config.HTTP_REQUEST_TIMEOUT
        )
        print(str(response.text))
        
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': 'x-api-key, Authorization'
        },
        'body': response.text
    }

def getAppleAdgroups(event, context):
    print('Loading getAppleAdgroups....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    campaign_id = queryStringParameters["campaign_id"]
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    client : Client = DynamoUtils.getClient(dynamodb, org_id)

    # handle auth token
    if client.auth is not None:
        print("found auth values in client " + str(client.auth))
        authToken = LambdaUtils.getAuthToken(client.auth, client.asaId)
        
        # get campaigns
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_ADGROUPS_URL % campaign_id
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % client.asaId}
        print("URL is" + url)
        print("Headers are" + str(headers))
        response = requests.get(
            url,
            headers=headers,
            timeout=config.HTTP_REQUEST_TIMEOUT
        )
        print(str(response.text))
        
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': 'x-api-key, Authorization'
        },
        'body': response.text
    }


# NOTE remove this unused
# def getAppleAcls(event, context):
#     print('Loading getAppleAcls....')
#     print("Received event: " + json.dumps(event, indent=2))
#     print("Received context: " + str(context))
#     queryStringParameters = event["queryStringParameters"]
#     org_id = queryStringParameters["org_id"]
#     dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
#     client: Client = DynamoUtils.getClient(dynamodb, org_id)

#     print(json.dumps(client.toJSON()))
    
#     # handle auth token
#     if client.auth is not None:
#         print("found auth values in client " + str(client.auth))
#         authToken = LambdaUtils.getAuthToken(client.auth, client.orgId)

#     headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % client.orgId}
#     get_acls_response = requests.get(config.APPLE_SEARCHADS_URL_BASE_V4 + "acls",
#         headers=headers
#     )

#     #extract all the apps assignd to the apple search ads account
#     get_acls_all_orgs_response = json.loads(get_acls_response.text)

#     print(str(get_acls_all_orgs_response))
#     get_acls_all_orgs_list = [get_acls_all_orgs_response[x] for x in get_acls_all_orgs_response]
#     get_acls_all_orgs_list_extracted = get_acls_all_orgs_list[0][0:1000]
#     acls_response = list(
#         filter(
#             lambda org:(org["orgId"] == int(client.orgId)), get_acls_all_orgs_list_extracted
#         )
#     )
        
#     return {
#         'statusCode': 200,
#         'headers': {
#             'Access-Control-Allow-Origin': '*',
#             'Access-Control-Allow-Methods': '*',
#             'Access-Control-Allow-Headers': 'x-api-key, Authorization'
#         },
#         'body': json.dumps(acls_response)
#     }

# TODO remove this from front end
def getAppleAuth(event, context):
    print('Loading getAppleAuth....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    dynamodb = LambdaUtils.getApiEnvironmentDetails(event).get('dynamodb')
    client: Client = DynamoUtils.getClient(dynamodb, org_id)
    if client.auth is not None:
        print("found auth values in client " + str(client.auth))
        authToken = LambdaUtils.getAuthToken(client.auth, client.asaId)
        
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': '*',
            'Access-Control-Allow-Headers': 'x-api-key, Authorization'
        },
        'body': json.dumps(authToken)
    }