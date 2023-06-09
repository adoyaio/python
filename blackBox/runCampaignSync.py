import decimal
import json
from utils import LambdaUtils, S3Utils
import requests
import boto3
import sys
from Client import Client

def initialize(clientEvent): 
    global sendG
    global clientG
    global userG
    global emailToG
    global dynamodb
    global logger
    global authToken

    emailToG = clientEvent['rootEvent']['emailToInternal']
    sendG = LambdaUtils.getSendG(
        clientEvent['rootEvent']['env']
    )
    dynamodb = LambdaUtils.getDynamoResource(
        clientEvent['rootEvent']['env'],
        clientEvent['rootEvent']['dynamoEndpoint']
    )

    clientG = Client.buildFromOrgdetails(
        json.loads(
            clientEvent['orgDetails']
        )
    )
    authToken = clientEvent['authToken']
    logger = LambdaUtils.getLogger(
        clientEvent['rootEvent']['env']
    ) 
    logger.info("runCampaignSync:::initialize(), rootEvent='" + str(clientEvent['rootEvent']))

def process():
    print("runCampaignSync:::" + clientG.clientName + ":::" + str(clientG.orgId))
    asaId = clientG.asaId
    orgId = clientG.orgId
    pemFilename = clientG.pemFilename
    keyFilename = clientG.keyFilename

    auth = clientG.auth
    if auth is not None:
        authToken = LambdaUtils.getAuthToken(auth, asaId)
        headers = {
            "Authorization": "Bearer %s" % authToken, 
            "X-AP-Context": "orgId=%s" % asaId,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    else:
        headers = {"Authorization": "orgId=%s" % asaId}
        
    appleCampaigns: list = clientG.appleCampaigns
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
        # DEPRECATED TODO remove
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


    # write to dynamo
    updated = json.loads(clientG.toJSON(), parse_float=decimal.Decimal)
    updated['orgId'] = updated.get('asaId')
    updated.pop('asaId') # in clients json there is no asaId key, orgId on orgdetail represents asaid

    table = dynamodb.Table('clients_2')
    table.put_item(
        Item = {
                'orgId': orgId,
                'orgDetails': updated
            }
    )

if __name__ == "__main__":
    clientEvent = LambdaUtils.getClientForLocalRun(
        str(sys.argv[1]),
        ['james@adoya.io']
    )
    initialize(clientEvent)
    process()


def lambda_handler(clientEvent):
    initialize(clientEvent)
    try: 
        process()
    except:
        e = sys.exc_info()[0]
        return {
            'statusCode': 400,
            'body': json.dumps('Run Campaign Sync Failed: ' + str(e))
        }
    return {
        'statusCode': 200,
        'body': json.dumps('Run Campaign Sync Complete')
    }