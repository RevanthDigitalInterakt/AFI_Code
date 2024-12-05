import requests
import os
from fastapi import APIRouter, HTTPException
from sourcecode.crmAuthentication import authenticate_crm  
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Create the FastAPI router instance
router = APIRouter()

# Load environment variables from the .env file
load_dotenv()

# Fetch necessary environment variables from .env
CRM_API_URL = "https://afi-group.crm11.dynamics.com/"  # Hardcoded for now
CRM_TOKEN_URL="https://login.microsoftonline.com/11abf71b-55aa-4f1e-8a9d-4a801bdbee28/oauth2/token"
# CRM_API_URL="https://afi-group.crm11.dynamics.com/api/data/v9.0"
MOENGAGE_API_URL = "https://api-02.moengage.com/v1/transition/6978DCU8W19J0XQOKS7NEE1C_DEBUG"

CRM_CLIENT_ID = "111ed0aa-ce80-4ed6-a4da-d1d1bba1aeac"
CRM_CLIENT_SECRET = "lT28Q~oySx.eCcPGKGn0FVlg1TeBUdDYz1ec~bnb"
CRM_RESOURCE = "https://afi-group.crm11.dynamics.com"
# Debugging prints to check if environment variables are loaded correctly
print(f"CRM_API_URL: {CRM_API_URL}")

moe_token="Njk3OERDVThXMTlKMFhRT0tTN05FRTFDX0RFQlVHOjhiWk9TcEs3UTloRTl4cnV3ck5ZR0JodQ=="
token_moe=f'Basic {moe_token}'

# Define the fetch_leads function
@router.get("/fetch-leads")
async def fetch_leads():
    print("Entered fetch-leads")
    """Fetch leads from Dynamics 365 CRM using the access token."""
    try:
        # Authenticate and get the access token
        token = await authenticate_crm()  # Ensure you're awaiting the async function
        if not token:
            raise HTTPException(status_code=401, detail="Failed to retrieve access token")
        print(f"Token: {token}")

        # Prepare the headers for the request
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Get the current time and subtract one hour to get the time range
        one_hour_ago = (datetime.utcnow() - timedelta(days=10))

        # Format the DateTimeOffset correctly for CRM API (including UTC timezone)
        formatted_time = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # Exclude extra microseconds and add 'Z' for UTC

        print(f"Formatted time: {formatted_time}")

        # Set the API endpoint to fetch leads from Dynamics 365 CRM # top 5 is set up here for dev
        leads_url = f"{CRM_API_URL}/api/data/v9.0/leads?$filter=createdon ge {formatted_time}&$top=5&$select=lastname,new_afileadscore,_parentcontactid_value,_parentaccountid_value,companyname,mobilephone,telephone1,emailaddress1,new_leadtype,leadsourcecode,new_utm_campaign,new_utm_campaignname,new_utm_content,new_utm_source,new_utm_medium,new_utm_term,new_utm_keyword,createdon,_ownerid_value,statuscode,subject"

        # Initialize an empty list to store leads
        all_leads = []

        # Fetch leads with pagination
        while leads_url:
            response = requests.get(leads_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                all_leads.extend(data.get("value", []))  # Add the leads to the list

                # Check if there are more leads to fetch
                leads_url = data.get("@odata.nextLink")
                
                # Debugging output for pagination
                if leads_url:
                    print(f"Fetching more leads from {leads_url}")
            else:
                # Log the response content for debugging
                print(f"Failed to fetch leads: {response.status_code} - {response.text}")
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch leads from CRM.")
        
        # Return the aggregated leads
        return {"leads": all_leads}
    
    except Exception as e:
        # Log any error that occurs
        print(f"Error during fetch-leads: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


def map_lead_to_moengage(lead):
    # Map fields from CRM to MoEngage with appropriate checks and mapping
    payload = {
        "leadid": lead.get("leadid"),
        "u_em": lead.get("emailaddress1"),  
        "u_mb": lead.get("mobilephone"),  
        "telephone1": lead.get("telephone1"), 
        "Company Name": lead.get("companyname" ),  # Company name
        # "statuscode": lead.get("statuscode" ),  # Status code
        # "Lead Type": lead.get("new_leadtype" ),  # Lead type
        # "Lead Source Code": lead.get("leadsourcecode" ),  # Lead source
        "new_utm_campaign": lead.get("new_utm_campaign" ),  # UTM Campaign
        "new_utm_campaignname": lead.get("new_utm_campaignname" ),  # UTM Campaign Name
        "new_utm_content": lead.get("new_utm_content" ),  # UTM Content
        "new_utm_source": lead.get("new_utm_source" ),  # UTM Source
        "new_utm_medium": lead.get("new_utm_medium" ),  # UTM Medium
        "new_utm_term": lead.get("new_utm_term" ),  # UTM Term
        "new_utm_keyword": lead.get("new_utm_keyword" ),  # UTM Keyword
        "Created On": lead.get("createdon" ),  # Created date
        # "Owner": lead.get("_ownerid_value"),  # Owner ID
        "Topic": lead.get("subject"),  
        # "Parent Contact for lead": lead.get("_parentcontactid_value"),  
        # "Parent Account for lead": lead.get("_parentaccountid_value"),  

    }
    
    customer_id=payload.get("u_em")
    final_payload={
        "type": "transition",
        "elements": [
            {
                "type": "customer",
                "customer_id": customer_id,
                "attributes": payload,
            },
            {
                "type": "event",
                "customer_id": customer_id,
                "device_id": "96bd03b6-defc-4203-83d3-dc1c73080232",  # Replace with actual device ID if available
                "actions": []  # Empty actions array as per your example
            },
        ],
    }
    
    return final_payload



async def send_to_moengage(leads):

    
    headers = {
        'Authorization':token_moe ,
        'Content-Type': 'application/json',
        'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C_DEBUG'
    }

    for lead in leads:
        payload = map_lead_to_moengage(lead)
        response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)
        if response.status_code == 200:
            print(f"Lead {lead['emailaddress1']} sent successfully")
        else:
            print(f"Failed to send lead {lead['emailaddress1']}: {response.text}")

# Endpoint to fetch and send leads to MoEngage
@router.get("/sync-leads")
async def sync_leads():
    try:
        leads_response = await fetch_leads()
        leads = leads_response.get("leads", [])

        # Send leads to MoEngage
        await send_to_moengage(leads)

        return {"status": "Leads synchronized successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")