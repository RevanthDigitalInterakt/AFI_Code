import requests
from fastapi import APIRouter, HTTPException,Query
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta
import boto3
import json

router = APIRouter()

# Initialize the AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")

def get_secret(secret_name: str):
    """Fetch secrets from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        elif "SecretBinary" in response:
            return json.loads(response["SecretBinary"])
    except Exception as e:
        raise HTTPException(status_code=404, detail="Secrets not found")

# Load secrets
secrets = get_secret("afi/crm/test")

if secrets:
    CRM_API_URL = secrets.get("CRM_API_URL", "")
    CRM_TOKEN_URL = secrets.get("CRM_TOKEN_URL", "")
    CRM_CLIENT_ID = secrets.get("CRM_CLIENT_ID", "")
    CRM_CLIENT_SECRET = secrets.get("CRM_CLIENT_SECRET", "")
    MOENGAGE_API_URL = secrets.get("MOENGAGE_API_URL", "")
    moe_token = secrets.get("moe_token", "")
else:
    raise HTTPException(status_code=500, detail="Failed to load secrets")

# Authorization token for MoEngage
token_moe = f"Basic {moe_token}"


global_token=None




@router.get("/fetch-leads")
async def fetch_leads():

    global global_token


    try:
        # Authenticate and get the access token
        token = await authenticate_crm()
        global_token=token  # Ensure you're awaiting the async function
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
        leads_url = f"{CRM_API_URL}/api/data/v9.0/leads?$filter=createdon ge {formatted_time}&$top=10&$select=lastname,new_afileadscore,_parentcontactid_value,_parentaccountid_value,companyname,mobilephone,telephone1,emailaddress1,new_leadtype,leadsourcecode,new_utm_campaign,new_utm_campaignname,new_utm_content,new_utm_source,new_utm_medium,new_utm_term,new_utm_keyword,createdon,_ownerid_value,statuscode,subject&$expand=parentcontactid($select=emailaddress1),parentaccountid($select=accountnumber,)"

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


async def map_lead_to_moengage(lead):
    

    new_leadtype_metadata_response = await fetch_metadata("new_leadtype")
    new_leadtype_options = {
        option["value"]: option["label"]
        for option in new_leadtype_metadata_response["options"]
    }

    statuscode_metadata_response = await fetch_statuscode_metadata("statuscode")
    statuscode_options = {
        option["value"]: option["label"]
        for option in statuscode_metadata_response["options"]
    }

    leadsourcecode_metadata_response = await fetch_leadsourcecode_metadata("leadsourcecode")
    leadsourcecode_options = {
        option["value"]: option["label"]
        for option in leadsourcecode_metadata_response["options"]
    }

    # Extract the fields from the lead and map them to the corresponding labels using metadata
    lead_type = new_leadtype_options.get(lead.get("new_leadtype"), "Unknown Lead Type")
    lead_source = leadsourcecode_options.get(lead.get("leadsourcecode"), "Unknown Lead Source")
    lead_status = statuscode_options.get(lead.get("statuscode"), "Unknown Status")
    print("calling parent account and contact id")
    parent_contact=lead.get("parentcontactid")
    parent_account=lead.get("parentaccountid")
    parent_contact_email = lead.get("parentcontactid", {}).get("emailaddress1", "No Contact Email") if parent_contact  else None
    parent_account_number = lead.get("parentaccountid", {}).get("accountnumber", "No Account Number") if parent_account  else None

    print(parent_account_number,"\tseparater\t",parent_contact_email)

    print("printing other attributes")
    print(lead_type)
    print(lead_status)
    print(lead_status)

    print("owner id")
    email_data_response = await fetch_email_from_lead()
    internal_email_address = email_data_response["internal_email_address"]
    print("check email here\n")
    print(internal_email_address)

    payload = {
        "leadid": lead.get("leadid"),
        "u_em": lead.get("emailaddress1"),  
        "u_mb": lead.get("mobilephone"),  
        "telephone1": lead.get("telephone1"), 
        "Company Name": lead.get("companyname" ),  # Company name
        "Lead Type": lead_type,  # Use the mapped value
        "Lead Source Code": lead_source,  # Use the mapped value
        "Status Code": lead_status,  # Use the mapped value
        "new_utm_campaign": lead.get("new_utm_campaign" ),  # UTM Campaign
        "new_utm_campaignname": lead.get("new_utm_campaignname" ),  # UTM Campaign Name
        "new_utm_content": lead.get("new_utm_content" ),  # UTM Content
        "new_utm_source": lead.get("new_utm_source" ),  # UTM Source
        "new_utm_medium": lead.get("new_utm_medium" ),  # UTM Medium
        "new_utm_term": lead.get("new_utm_term" ),  # UTM Term
        "new_utm_keyword": lead.get("new_utm_keyword" ),  # UTM Keyword
        "Created On": lead.get("createdon" ),  # Created date
        # "Owner": lead.get("_ownerid_value"),  # Owner ID
        "Owner": internal_email_address,
        "Topic": lead.get("subject"),  
        "Parent Contact Email": parent_contact_email,  # Parent contact email
        "Parent Account Number": parent_account_number  # Parent account number
        # "Parent Contact for lead": lead.get("_parentcontactid_value"),  
        # "Parent Account for lead": lead.get("_parentaccountid_value"),  

    }

    print("caling meta data for other attributes")
    
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
    print(final_payload)
    
    return final_payload



async def send_to_moengage(leads):

    
    headers = {
        'Authorization':token_moe ,
        'Content-Type': 'application/json',
        'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C_DEBUG'
    }

    for lead in leads:
        print("check here\n")
        print(lead)
        payload = await map_lead_to_moengage(lead)
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
    



async def fetch_metadata(attribute: str = Query("new_leadtype", description="Logical name of the attribute to fetch metadata for")):
    
    global global_token


    token = global_token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # URL template with the required query expansion for detailed attribute metadata
    metadata_url = f"{CRM_API_URL}/api/data/v9.0/EntityDefinitions(LogicalName='lead')/Attributes(LogicalName='{attribute}')/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet"

    try:
        # Fetch metadata
        response = httpx.get(metadata_url, headers=headers)
        response.raise_for_status()

        # Extract and return relevant parts of the response
        data = response.json()
        attribute_display_name = data.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", "No label found")
        options = [
            {
                "value": option.get("Value"),
                "label": option.get("Label", {}).get("UserLocalizedLabel", {}).get("Label", "No label found"),
            }
            for option in data.get("OptionSet", {}).get("Options", [])
        ]

        return {
            "attribute": attribute,
            "display_name": attribute_display_name,
            "options": options,
        }

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"HTTP Error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching metadata: {str(e)}")


@router.get("/fetch-statuscode-metadata")
async def fetch_statuscode_metadata(attribute: str = Query("statuscode", description="Logical name of the attribute to fetch metadata for")):
   

    global global_token


    token = global_token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    metadata_url = f"{CRM_API_URL}/api/data/v9.2/EntityDefinitions(LogicalName='lead')/Attributes(LogicalName='{attribute}')/Microsoft.Dynamics.CRM.StatusAttributeMetadata?$expand=OptionSet"

    try:
        # Fetch metadata
        response = httpx.get(metadata_url, headers=headers)
        response.raise_for_status()

        # Extract and return relevant parts of the response
        data = response.json()

        # Check if OptionSet exists in the response
        if "OptionSet" not in data or not data["OptionSet"].get("Options"):
            raise HTTPException(status_code=404, detail="OptionSet not found or empty in the response")

        data = response.json()
        attribute_display_name = data.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", "No label found")
        # Extract options and labels
        options = [
            {
                "value": option.get("Value"),
                "label": option.get("Label", {}).get("UserLocalizedLabel", {}).get("Label", "No label found"),
            }
            for option in data["OptionSet"].get("Options", [])
        ]

        return {
            "attribute": attribute,
            "display_name": attribute_display_name,
            "options": options,
        }

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"HTTP Error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching statuscode metadata: {str(e)}")

@router.get("/fetch-leadsourcecode-metadata")
async def fetch_leadsourcecode_metadata(attribute: str = Query("leadsourcecode", description="Logical name of the attribute to fetch metadata for")):
   


    global global_token


    token = global_token


    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    metadata_url = f"{CRM_API_URL}/api/data/v9.0/EntityDefinitions(LogicalName='lead')/Attributes(LogicalName='{attribute}')/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet"

    try:
        # Fetch metadata
        response = httpx.get(metadata_url, headers=headers)
        response.raise_for_status()

        # Extract and return relevant parts of the response
        data = response.json()
        attribute_display_name = data.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", "No label found")
        options = [
            {
                "value": option.get("Value"),
                "label": option.get("Label", {}).get("UserLocalizedLabel", {}).get("Label", "No label found"),
            }
            for option in data.get("OptionSet", {}).get("Options", [])
        ]

        return {
            "attribute": attribute,
            "display_name": attribute_display_name,
            "options": options,
        }

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"HTTP Error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching leadsourcecode metadata: {str(e)}")




async def fetch_email_from_lead():

    global global_token
    try:
        # Get the token dynamically
        token = global_token
        
        # Headers with the dynamic token
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Step 1: Fetch _ownerid_value from the Leads API
        leads_url = (
            f"{CRM_API_URL}/api/data/v9.0/leads"
            "?$select=_ownerid_value"
            "&$orderby=createdon desc"
        )
        leads_response = requests.get(leads_url, headers=headers)
        # print("Leads Response:", leads_response.status_code, leads_response.text)  
        leads_response.raise_for_status()
        
        leads_data = leads_response.json()
        if not leads_data.get("value"):
            raise HTTPException(status_code=404, detail="No leads found.")
        
        # Get the first _ownerid_value
        _ownerid_value = leads_data["value"][0].get("_ownerid_value")
        if not _ownerid_value:
            raise HTTPException(status_code=404, detail="_ownerid_value not found in the lead.")


        system_user_url = (
            f"{CRM_API_URL}/api/data/v9.0/systemusers"
            f"?$filter=systemuserid eq {_ownerid_value}"
            "&$select=internalemailaddress,fullname"
        )
        system_user_response = requests.get(system_user_url, headers=headers)
        # print("System User Response:", system_user_response.status_code, system_user_response.text) 
        system_user_response.raise_for_status()

        system_user_data = system_user_response.json()
        if not system_user_data.get("value"):
            raise HTTPException(status_code=404, detail="No system user found with the given _ownerid_value.")

        # Extract the email address
        internal_email_address = system_user_data["value"][0].get("internalemailaddress")
       
        if not internal_email_address:
            raise HTTPException(status_code=404, detail="Internal email address not found.")

        return {
        
            "internal_email_address": internal_email_address,
        }

    except requests.RequestException as e:
        print("Error:", str(e))  # Debugging line
        raise HTTPException(status_code=500, detail=str(e))
