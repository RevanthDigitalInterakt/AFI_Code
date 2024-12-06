import requests
from fastapi import APIRouter, HTTPException
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
        print(f"Error fetching secret: {e}")
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



@router.get("/fetch")
async def fetch_contacts():
    """Fetch contacts from Dynamics 365 CRM using the access token."""
    try:
        token = await authenticate_crm()
        if not token:
            raise HTTPException(status_code=401, detail="Failed to retrieve access token")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
         
            }

        query="emailaddress1,_accountid_value,_parentcustomerid_value,telephone1,mobilephone,jobtitle,firstname,address1_city,lastname,address1_line1,address1_line2,address1_line3,address1_postalcode,donotemail,donotphone,new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,data8_tpsstatus,new_lastmewpscall,new_lastmewpscallwith,new_lastemailed,new_lastemailedby,new_lastcalled,new_lastcalledby,new_registerforupliftonline,createdon,preferredcontactmethodcode"       
        ten_days_ago = (datetime.utcnow() - timedelta(days=10)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        contacts_url = f"{CRM_API_URL}/api/data/v9.0/contacts?$filter=modifiedon ge {ten_days_ago}&$top=2&$select={query}"
        all_contacts = []
        print("just eneterd contacts")
        while contacts_url:
            response = requests.get(contacts_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                all_contacts.extend(data.get("value", []))
                contacts_url = data.get("@odata.nextLink")
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch contacts from CRM.")
        print("prinitng all contacts")
        print(all_contacts)
        return {"contacts": all_contacts}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


def map_contact_to_moengage(contact):
   

    print("fetching Account number for contacts")
  
    parent_contact=contact.get("parentcustomerid_account")
    _accountid_value = contact.get("parentcustomerid_account", {}).get("accountnumber", "No Account Number") if parent_contact  else None
    _parentcustomerid_value = contact.get("parentcustomerid_account", {}).get("name", "No Account Name") if parent_contact  else None




    attributes= {
        # "u_n": contact.get("fullname"),
        "u_em": contact.get("emailaddress1"),
        "u_mb": contact.get("mobilephone"),
        "telephone1": contact.get("telephone1"),
        "Created On": contact.get("createdon"),
        "Modified On": contact.get("modifiedon"),
        "new_contacttype": contact.get("new_contacttype"),
        # "_accountid_value": contact.get("_accountid_value"),
        # "_parentcustomerid_value": contact.get("_parentcustomerid_value"),
        "_accountid_value": _accountid_value,  # Parent contact email
        "_parentcustomerid_value": _parentcustomerid_value,
        "jobtitle": contact.get("jobtitle"),
        "u_fn": contact.get("firstname"),
        "u_ln": contact.get("lastname"),
        "address1_city": contact.get("address1_city"),
        "address1_line1": contact.get("address1_line1"),
        "address1_line2": contact.get("address1_line2"),
        "address1_line3": contact.get("address1_line3"),
        "address1_postalcode": contact.get("address1_postalcode"),
        "donotemail": contact.get("donotemail"),
        "donotphone": contact.get("donotphone"),
        "new_afiupliftemail": contact.get("new_afiupliftemail"),
        "new_underbridgevanmountemail": contact.get("new_underbridgevanmountemail"),
        "new_rapidemail": contact.get("new_rapidemail"),
        "new_rentalsspecialoffers": contact.get("new_rentalsspecialoffers"),
        "new_resaleemail": contact.get("new_resaleemail"),
        "new_trackemail": contact.get("new_trackemail"),
        "new_truckemail": contact.get("new_truckemail"),
        "new_utnemail": contact.get("new_utnemail"),
        "new_hoistsemail": contact.get("new_hoistsemail"),
        "data8_tpsstatus": contact.get("data8_tpsstatus"),
        "new_lastmewpscall": contact.get("new_lastmewpscall"),
        "new_lastmewpscallwith": contact.get("new_lastmewpscallwith"),
        "new_lastemailed": contact.get("new_lastemailed"),
        "new_lastemailedby": contact.get("new_lastemailedby"),
        "new_lastcalled": contact.get("new_lastcalled"),
        "new_lastcalledby": contact.get("new_lastcalledby"),
        "new_registerforupliftonline": contact.get("new_registerforupliftonline"),
        "preferredcontactmethodcode": contact.get("preferredcontactmethodcode"),
    }
    print("printing attriutes")
    # attributes['u_em']="jhon@example.com"
    print(attributes)
    customer_id=attributes.get("u_em")
    print(customer_id)
    final_payload={
        "type": "transition",
        "elements": [
            {
                "type": "customer",
                "customer_id": customer_id,
                "attributes": attributes,
            },
            {
                "type": "event",
                "customer_id": customer_id,
                "actions": []  # Empty actions array as per your example
            },
        ],
    }
    print("printing final payload")
    print(final_payload)
    
    return final_payload


@router.get("/sync")
async def sync_contacts():
    """Fetch contacts from CRM and send them to MoEngage."""
    try:
        contacts_response = await fetch_contacts()
        contacts = contacts_response.get("contacts", [])
        print("printing token")
        print(moe_token)
        headers = {
            'Authorization': token_moe,
            'Content-Type': 'application/json',
            'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C_DEBUG'
        }

        # Send contacts to MoEngage
        for contact in contacts:
            payload = map_contact_to_moengage(contact)
            print("printing payload")
            print(payload)
            response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)
            if response.status_code == 200:
                print(f"Contact sent successfully for {contact['emailaddress1']} ")
            else:
                print(f"Failed to send contact {contact['emailaddress1']}: {response.text}")

        return {"status": "Contacts synchronized successfully"}
    


    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
