import decimal
import json
from utils import LambdaUtils, S3Utils
import requests
import boto3

with open("./data/dynamo/clients.json") as json_file:
    clients = json.load(json_file)
    client: dict
    for client in clients:
        orgId = client['orgId']
        pemFilename = client.get('pemFilename', None)
        keyFilename = client.get('keyFilename', None)
        auth = client.get('auth', None)

        if auth is not None:
            authToken = LambdaUtils.getAuthToken(auth)
            headers = {
                "Authorization": "Bearer %s" % authToken, 
                "X-AP-Context": "orgId=%s" % orgId,
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
        else:
            headers = {"Authorization": "orgId=%s" % orgId}
            
        appleCampaigns: list = client.get('appleCampaigns')
        appleCampaign: dict
        for appleCampaign in appleCampaigns:
            # print("found campaign:", str(appleCampaign))
            campaignId = appleCampaign.get('campaignId')
            url = "https://api.searchads.apple.com/api/v4/campaigns/%s" % campaignId

            print("Apple URL is" + url)
            print("Headers are" + str(headers))

            if auth is not None:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=800
                )
            else:
                response = requests.get(
                    url,
                    cert=(S3Utils.getCert(pemFilename), S3Utils.getCert(keyFilename)),
                    headers=headers,
                    timeout=800
                )

            print("The result of GET campaign to Apple: %s" % response)
            
            responseDict:dict = json.loads(response.text)

            # from campaign endpoint
            campaignName = responseDict['data']['name']
            dailyBudget = responseDict['data']['dailyBudgetAmount']['amount']
            lifetimeBudget = responseDict['data']['budgetAmount']['amount']
            status = responseDict['data']['status']
            
            # adgroup endpoint
            adgroupId = appleCampaign.get('adGroupId')
            url = "https://api.searchads.apple.com/api/v4/campaigns/%s/adgroups/%s" % (campaignId, adgroupId )
            print("Apple URL is" + url)
            print("Headers are" + str(headers))

            if auth is not None:
                adgroupResponse = requests.get(
                    url,
                    headers=headers,
                    timeout=800
                )
            else:
                adgroupResponse = requests.get(
                    url,
                    cert=(S3Utils.getCert(pemFilename), S3Utils.getCert(keyFilename)),
                    headers=headers,
                    timeout=800
                )

            print("The result of GET aggroup to Apple: %s" % adgroupResponse)
            
            adgroupResponseDict:dict = json.loads(adgroupResponse.text)
            print("ad group response")
            print(str(adgroupResponseDict))

            adgroupData:dict = adgroupResponseDict['data']
            
            print("ad group data")
            print(str(adgroupData))

            targetingDimensions:dict = adgroupData.get('targetingDimensions', None)            
            adGroupName = adgroupData['name']
           
            # default vals
            genderIncluded = ["M","F"]
            minAge = None
            maxAge = None
            if targetingDimensions is not None:
                gender:dict = targetingDimensions.get('gender', None)
                age:dict = targetingDimensions.get('age')
                if gender is not None:
                    genderIncluded:list = gender.get('included', [])
                else:
                    genderIncluded:list = ["M", "F"]
                if age is not None:
                    minAge = age.get('included')[0]['minAge']
                    maxAge = age.get('included')[0]['maxAge']
                else:
                    minAge = None
                    maxAge = None

            
            # write values to campaign
            appleCampaign['campaignName'] = campaignName
            appleCampaign['dailyBudget'] = float(dailyBudget)
            appleCampaign['lifetimeBudget'] = float(lifetimeBudget)
            appleCampaign['status'] = status
            appleCampaign['gender'] = genderIncluded
            appleCampaign['minAge'] = minAge
            appleCampaign['maxAge'] = maxAge
            appleCampaign['adGroupName'] = adGroupName

with open('clients.fixed.json', 'w') as outfile:
    json.dump(clients, outfile, indent=4, sort_keys=True) 