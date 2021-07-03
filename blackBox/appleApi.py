import decimal
import boto3
import json
import time
import datetime
import sys
from utils.debug import debug, dprint
from utils import DynamoUtils, ApiUtils, EmailUtils, LambdaUtils
from configuration import config
from utils.DecimalEncoder import DecimalEncoder
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from Client import Client
import requests


# def getAppleKeys(event, context):
#     print('Loading getAppleKeys....')
#     print("Received event: " + json.dumps(event, indent=2))
#     print("Received context: " + str(context))
#     print("Received context: " + str(context.client_context))
#     # queryStringParameters = event["queryStringParameters"]
#     # org_id = queryStringParameters["org_id"]

#     # dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
#     # client = DynamoUtils.getClient(dynamodb, org_id)
#     private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
#     public_key = private_key.public_key()
#     # serializing into PEM
#     ec_key = private_key.private_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.OpenSSH, encryption_algorithm=ec.SECP256R1)
#     ec_pem = public_key.public_bytes(encoding=serialization.Encoding.PEM, format=serialization.PublicFormat.OpenSSH)
#     print(ec_pem.decode())
#     print(ec_key.decode())

#     returnKey = ec_key.decode()
#     returnPem = ec_pem.decode()

#     return {
#         'statusCode': 200,
#         'headers': {
#             'Access-Control-Allow-Origin': '*',
#             'Access-Control-Allow-Methods': 'GET',
#             'Access-Control-Allow-Headers': 'x-api-key'
#         },
#         'body': { 'privateKey': returnKey, 'publicKey': returnPem }
#     }

def postAppleCampaign(event, context):
    print('Loading postAppleCampaign....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received context: " + str(context.client_context))

    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]
    
    # get token 
    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
    client = DynamoUtils.getClient(dynamodb, org_id)

    # handle auth token
    print(str(client))
    auth = client[0].get('orgDetails').get('auth', None)

    if auth is None:
        return {
        'statusCode': 400,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': {}
    }

    if auth is not None:
        print("found auth values in client " + str(auth))
        authToken = LambdaUtils.getAuthToken(auth)


    # create competitor campaign
    campaignData = json.loads(event["body"])
    competitorCreated: bool = createCampaign('competitor', campaignData, authToken)

    if not competitorCreated: 
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'x-api-key'
            },
            'body': {}
        }

    # create brand campaign
    brandCreated: bool = createCampaign('brand', campaignData, authToken)

    if not brandCreated: 
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'x-api-key'
            },
            'body': {}
        }

    # create category campaign
    categoryCreated: bool = createCampaign('category', campaignData, authToken)

    if not categoryCreated: 
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'x-api-key'
            },
            'body': {}
        }

    # create exact discover campaign
    exactDiscoveryCreated: bool = createCampaign('exact_discovery', campaignData, authToken)

    if not exactDiscoveryCreated: 
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'x-api-key'
            },
            'body': {}
        }

    # create broad discover campaign
    broadDiscoveryCreated: bool = createCampaign('broad_discovery', campaignData, authToken)

    if not broadDiscoveryCreated: 
        return {
            'statusCode': 400,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST',
                'Access-Control-Allow-Headers': 'x-api-key'
            },
            'body': {}
        }

    # create search discover campaign
    searchDiscoveryCreated: bool = createCampaign('search_discovery', campaignData, authToken)

    if not searchDiscoveryCreated: 
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
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': {}
    }

def createCampaign(campaignType, campaignData, authToken):
    # parse event data
    org_id = campaignData["org_id"]
    app_name = campaignData["app_name"]
    adam_id = campaignData["adam_id"]

    # campaign level 
    campaign_target_country = campaignData["campaign_target_country"]
    front_end_lifetime_budget = float(campaignData["front_end_lifetime_budget"])
    front_end_daily_budget = float(campaignData["front_end_daily_budget"])
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
        lifetime_budget = float(front_end_lifetime_budget) * 0.30
        daily_budget_amount = float(front_end_daily_budget) * 0.30
    elif campaignType == 'category':
        lifetime_budget = float(front_end_lifetime_budget) * 0.30
        daily_budget_amount = float(front_end_daily_budget) * 0.30
    elif campaignType == 'brand':
        lifetime_budget = float(front_end_lifetime_budget) * 0.15
        daily_budget_amount = float(front_end_daily_budget) * 0.15
    elif campaignType == 'exact_discovery':
        lifetime_budget = float(front_end_lifetime_budget) * 0.05
        daily_budget_amount = float(front_end_daily_budget) * 0.05
    elif campaignType == 'broad_discovery':
        lifetime_budget = float(front_end_lifetime_budget) * 0.10
        daily_budget_amount = float(front_end_daily_budget) * 0.10
    elif campaignType == 'search_discovery':
        lifetime_budget = float(front_end_lifetime_budget) * 0.10
        daily_budget_amount = float(front_end_daily_budget) * 0.10
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
    base_url = "https://api.searchads.apple.com/api/v3/"


    # logic to ensure daily cap doesn't exceed total budget, apple will cause this to fail anyway but better to address up front
    if lifetime_budget > daily_budget_amount:
         print('Lifetime Budget Valid')
    elif lifetime_budget <= daily_budget_amount:
        # TODO bubble error
        print('Lifetime Budget Must Be Greater Than Daily Budget')
        print(str(lifetime_budget))
        print(str(daily_budget_amount))
        return False
    else:
        print("Invalid Lifetime Budget & Daily Cap Entry.")
        return False

    # logic to ensure target CPI doesn't exceed daily budget cap, apple will cause this to fail anyway but better to address up front
    if daily_budget_amount > target_cost_per_install:
        print('Daily Budget Valid')
    elif daily_budget_amount <= target_cost_per_install:
        print('Daily Budget Cap Must Be Greater Than Target Cost Per Install')
        print(str(daily_budget_amount))
        print(str(target_cost_per_install))
        return False
    else:
        print("Invalid Daily Cap & Max CPI Entry")
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


    # delay script briefly so apple api has enough time to update id in their system so our script can reference it
    time.sleep(5)

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
        "status": "PAUSED" # TODO ENABLED, set via environment 
    }

    create_campaign_url = base_url + "campaigns"

    print("Payload is '%s'." % create_campaign_payload)
    print("Url is '%s'." % create_campaign_url)
    create_campaign_response = requests.post(
        create_campaign_url,  
        json=create_campaign_payload,
        headers=headers
    )

    print ("The result of POST campaign to Apple: %s" % create_campaign_response)

    # error handling
    if create_campaign_response.status_code != 200:
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

    get_campaigns_url = base_url + "campaigns/find"

    print ("url is '%s'." % get_campaigns_url)
    print ("Payload is '%s'." % get_campaigns_payload)

    get_campaigns_response = requests.post(
        get_campaigns_url,
        json=get_campaigns_payload,
        headers=headers
    ) 

    print ("The result of get campaigns from apple : %s" % get_campaigns_response)

    # extract all the apps assignd to the apple search ads account
    campaign_id_data = json.loads(get_campaigns_response.text) 
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
            "defaultCpcBid": {
                "amount": str(round(target_cost_per_install * 0.50,2)),
                "currency": str(currency)
            },
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

    create_ad_group_url = base_url + "campaigns/%s/adgroups/" % new_campaign_id

    print ("Payload is '%s'." % create_ad_group_payload)
    print ("Url is '%s'." % create_ad_group_url)
    create_ad_group_response = requests.post(
        create_ad_group_url,
        json=create_ad_group_payload,
        headers=headers
    ) 

    print ("The result of POST adgroups to Apple: %s" % create_ad_group_response)

    # error handling
    if create_ad_group_response.status_code != 200:
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

    get_adgroups_url = base_url + "campaigns/%s/adgroups/find" % new_campaign_id
    print ("Url is '%s'." % get_adgroups_url)
    print ("Payload is '%s'." % get_adgroups_payload)

    get_adgroups_response = requests.post(
        base_url + "campaigns/%s/adgroups/find" % new_campaign_id,
        json=get_adgroups_payload,
        headers=headers
    ) 

    print ("The result of getting adgroups from Apple: %s" % get_adgroups_response)

    # extract all the ad groups
    ad_group_data = json.loads(get_adgroups_response.text) 
    ad_group_data_list = [ad_group_data[x] for x in ad_group_data]
    all_ad_groups_list = ad_group_data_list[0][0:1000]

    # the following returns a list of all the ad groups the output could be shown on the front end
    # all_ad_group_names = [d['name'] for d in all_ad_groups_list]

    # delay script briefly so apple api has enough time to update id in their system so our script can reference it
    time.sleep(5)

    #get the ad group which begins the keyword adding process
    new_ad_group_id = [aDict['id'] for aDict in all_ad_groups_list if aDict['name'] == ad_group_name][0]

    print("Created adgroup " + str(new_ad_group_id))

    if campaignType == 'exact_discovery':
        print("EXACT DISCOVERY campaign, skipping keywords")
        return True

    # create NEGATIVE keywords
    if campaignType == 'broad_discovery' or campaignType == 'search_discovery':
        time.sleep(20)
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

        create_negative_keyword_response = requests.post(
            negative_keyword_url,
            json=negative_keyword_payload,
            headers=headers
        )

        print("Headers are" + str(headers))
        print("Response headers" + str(create_negative_keyword_response.headers))
        print("Response is" + str(create_negative_keyword_response))
        print("Response text is" + str(create_negative_keyword_response.reason))
        print("The result of posting NEGATIVE keywords to Apple: %s" % create_negative_keyword_response)

        # error handling
        if create_negative_keyword_response.status_code != 200:
           return False

    
    # create new keywords and add to newly-created campaign and ad group
    if campaignType != 'search_discovery':
        time.sleep(10)
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
        targeted_keyword_url = base_url + "campaigns/%s/adgroups/%s/targetingkeywords/bulk" % (new_campaign_id, new_ad_group_id)
        print ("Url is '%s'." % targeted_keyword_url)
        print ("Payload is '%s'." % targeted_keyword_payload)
        create_targeted_keyword_response = requests.post(
            targeted_keyword_url,
            json=targeted_keyword_payload,
            headers=headers
        )

        print ("The result of posting keywords to Apple: %s" % create_targeted_keyword_response)

        # error handling
        if create_targeted_keyword_response.status_code != 200:
            return False


    return True


def getAppleApps(event, context):
    print('Loading getAppleApps....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
    client = DynamoUtils.getClient(dynamodb, org_id)

    # handle auth token
    print(str(client))
    auth = client[0].get('orgDetails').get('auth', None)
    if auth is not None:
        print("found auth values in client " + str(auth))
        authToken = LambdaUtils.getAuthToken(auth)
        url = config.APPLE_SEARCHADS_URL_BASE_V4 + config.APPLE_GET_APPS_URL
        headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % org_id}
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
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': response.text
    }

def getAppleAcls(event, context):
    print('Loading getAppleAcls....')
    print("Received event: " + json.dumps(event, indent=2))
    print("Received context: " + str(context))
    print("Received context: " + str(context.client_context))
    queryStringParameters = event["queryStringParameters"]
    org_id = queryStringParameters["org_id"]

    dynamodb = ApiUtils.getDynamoHost(event).get('dynamodb')
    client = DynamoUtils.getClient(dynamodb, org_id)

    # handle auth token
    print(str(client))
    auth = client[0].get('orgDetails').get('auth', None)
    if auth is not None:
        print("found auth values in client " + str(auth))
        authToken = LambdaUtils.getAuthToken(auth)

    headers = {"Authorization": "Bearer %s" % authToken, "X-AP-Context": "orgId=%s" % org_id}
    
    get_acls_response = requests.get(config.APPLE_SEARCHADS_URL_BASE_V4 + "acls",
        headers=headers
    )

    #extract all the apps assignd to the apple search ads account
    get_acls_all_orgs_response = json.loads(get_acls_response.text)

    print(str(get_acls_all_orgs_response))
    get_acls_all_orgs_list = [get_acls_all_orgs_response[x] for x in get_acls_all_orgs_response]
    get_acls_all_orgs_list_extracted = get_acls_all_orgs_list[0][0:1000]

    acls_response = list(
        filter(
            lambda org:(org["orgId"] == int(org_id)), get_acls_all_orgs_list_extracted
        )
    )
        
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET',
            'Access-Control-Allow-Headers': 'x-api-key'
        },
        'body': json.dumps(acls_response)
    }