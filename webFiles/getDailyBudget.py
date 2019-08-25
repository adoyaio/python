import datetime 
import json 
import os
import pandas as pd
import requests
import sys
import time
import urllib.parse

from cgiConfiguration import CERTIFICATES_DIR, DAILY_BUDGET_URL, HTTP_REQUEST_TIMEOUT



# ------------------------------------------------------------------------------
def getDailyBudget(client):
  org_id = client["orgId"]
  cert = (os.path.join(CERTIFICATES_DIR, client["pemFilename"]),
          os.path.join(CERTIFICATES_DIR, client["keyFilename"]))

  campaignIds = client["keywordAdderIds"]["campaignId"]
  search_match_campaign_id, broad_match_campaign_id, exact_match_campaign_id = \
    campaignIds["search"], campaignIds["broad"], campaignIds["exact"]

  headers = {f"Authorization": "orgId={orgId}"}

  payload = {"orderBy"    : [{ "field"     : "id",
                               "sortOrder" : "DESCENDING" }],
             "fields"     : ["id",
                             "name",
                             "adamId",
                             "budgetAmount",
                             "dailyBudgetAmount",
                             "status",
                             "servingStatus"],
             "conditions" : [{ "field"    : "servingStatus",        
                               "operator" : "IN",
                               "values"   : ["NOT_RUNNING"]
                             }],
             "pagination" : {"offset": 0, "limit": 1000}
            }

  search_match_campaign_budget_first_step = DAILY_BUDGET_URL % search_match_campaign_id
  broad_match_campaign_budget_first_step  = DAILY_BUDGET_URL % broad_match_campaign_id
  exact_match_campaign_budget_first_step  = DAILY_BUDGET_URL % exact_match_campaign_id


  ######this part pulls in search match campaign data######
  #response from api pull
  search_match_campaign_budget_second_step = requests.get(search_match_campaign_budget_first_step, cert=cert, json=payload, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)
  
  #data returned by api in string format
  search_match_campaign_budget_third_step = json.loads(search_match_campaign_budget_second_step.text) 
  
  #nested dictionary containing search match campaign data
  search_match_campaign_budget_fourth_step = search_match_campaign_budget_third_step["data"]
  search_match_campaign_budget_fourth_step['dailyBudgetAmount'] = search_match_campaign_budget_fourth_step['dailyBudgetAmount']['amount']
  search_match_campaign_budget_fifth_step = pd.DataFrame.from_records((search_match_campaign_budget_fourth_step),index=[0])
  
  
  ######this part pulls in broad match campaign data######
  #response from api pull
  broad_match_campaign_budget_second_step = requests.get(broad_match_campaign_budget_first_step, cert=cert, json=payload, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)
  
  #data returned by api in string format
  broad_match_campaign_budget_third_step = json.loads(broad_match_campaign_budget_second_step.text) 
  
  #nested dictionary containing broad match campaign data
  broad_match_campaign_budget_fourth_step = broad_match_campaign_budget_third_step["data"]
  broad_match_campaign_budget_fourth_step['dailyBudgetAmount'] = broad_match_campaign_budget_fourth_step['dailyBudgetAmount']['amount']
  broad_match_campaign_budget_fifth_step = pd.DataFrame.from_records((broad_match_campaign_budget_fourth_step),index=[0])
  
  
  ######this part pulls in exact match campaign data######
  #response from api pull
  exact_match_campaign_budget_second_step = requests.get(exact_match_campaign_budget_first_step, cert=cert, json=payload, headers=headers, timeout=HTTP_REQUEST_TIMEOUT)
  
  #data returned by api in string format
  exact_match_campaign_budget_third_step = json.loads(exact_match_campaign_budget_second_step.text) 
  
  #nested dictionary containing search match campaign data
  exact_match_campaign_budget_fourth_step = exact_match_campaign_budget_third_step["data"]
  exact_match_campaign_budget_fourth_step['dailyBudgetAmount'] = exact_match_campaign_budget_fourth_step['dailyBudgetAmount']['amount']
  exact_match_campaign_budget_fifth_step = pd.DataFrame.from_records((exact_match_campaign_budget_fourth_step),index=[0])
  
  #combine campaings into one dataframe for summarizing
  all_campaigns_first_step = [search_match_campaign_budget_fifth_step, broad_match_campaign_budget_fifth_step, exact_match_campaign_budget_fifth_step]
  all_campaigns_second_step = pd.concat(all_campaigns_first_step)
  
  #subset campaigns that are running
  all_campaigns_third_step = all_campaigns_second_step[(all_campaigns_second_step['servingStatus'] == 'RUNNING')]
  
  #convert budget to float for calculations
  all_campaigns_third_step['dailyBudgetAmount'] = all_campaigns_third_step['dailyBudgetAmount'].astype(float)
    
  #sum the budget
  daily_budget = all_campaigns_third_step['dailyBudgetAmount'].sum()

  return daily_budget



# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
if __name__ == "__main__":
  clientsArray = json.load(open("../../Data/clients.json"))
  for clientData in clientsArray:
    if clientData["clientName"] == "Covetly":
      print(getDailyBudget(clientData), file=sys.stderr) # Don't sent to stdout, in case someone runs this as a CGI-BIN script.
