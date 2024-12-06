import requests
from fastapi import APIRouter, HTTPException
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta
import boto3,json



router = APIRouter()

# Initialize Boto3 client for Secrets Manager
secrets_client = boto3.client('secretsmanager')

def get_secret(secret_name: str):
    """Retrieve secrets from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in response:
            return json.loads(response['SecretString'])
        elif 'SecretBinary' in response:
            return json.loads(response['SecretBinary'])
        else:
            raise ValueError("Secret format is not recognized")
    except Exception as e:
        print(f"Error fetching secret: {e}")
        return None  # Return None for error handling downstream

# Fetch secrets from AWS Secrets Manager
secrets = get_secret("afi/crm/test")

if secrets:
    CRM_API_URL = secrets.get("CRM_API_URL", "default_value")
    CRM_TOKEN_URL = secrets.get("CRM_TOKEN_URL", "default_value")
    CRM_CLIENT_ID = secrets.get("CRM_CLIENT_ID", "default_value")
    CRM_CLIENT_SECRET = secrets.get("CRM_CLIENT_SECRET", "default_value")
    MOENGAGE_API_URL = secrets.get("MOENGAGE_API_URL", "default_value")
    moe_token = secrets.get("moe_token", "default_value")
else:
    print("Failed to load secrets.")
    CRM_API_URL = CRM_TOKEN_URL = CRM_CLIENT_ID = CRM_CLIENT_SECRET = MOENGAGE_API_URL = moe_token = "default_value"

token_moe = f'Basic {moe_token}'

# Define global token
global_token = None





@router.get("/fetch")
async def fetch_accounts():
    """Fetch accounts from Dynamics 365 CRM using the access token."""
    try:
        token = await authenticate_crm()
        if not token:
            raise HTTPException(status_code=401, detail="Failed to retrieve access token")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Fetch accounts modified in the last 10 days
        query="new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,address1_city,sic,new_registration_no,_new_primaryhirecontact_value,new_lastinvoicedate,new_lasttrainingdate,new_groupaccountmanager,new_rentalam,donotphone,donotemail,new_afiupliftemail,new_underbridgevanmountemail,_new_primarytrainingcontact_value,address1_line1,address1_line2,address1_line3,creditlimit,new_twoyearsagorevenue,data8_tpsstatus,new_creditposition,new_lastyearrevenue,statuscode,address1_postalcode,new_accountopened,name,_new_primaryhirecontact_value,accountnumber,telephone1,emailaddress1,createdon,modifiedon"
        ten_days_ago = (datetime.utcnow() - timedelta(days=10)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        accounts_url = f"{CRM_API_URL}/api/data/v9.0/accounts?$filter=modifiedon ge {ten_days_ago}&$top=2&$select={query}"
        all_accounts = []

        while accounts_url:
            response = requests.get(accounts_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                all_accounts.extend(data.get("value", []))
                accounts_url = data.get("@odata.nextLink")
            else:
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch accounts from CRM.")

        return {"accounts": all_accounts}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


def map_account_to_moengage(account):

    parent_contact=account.get("parentcontactid")
    parent_account=account.get("parentaccountid")
    _new_primaryhirecontact_value = account.get("new_PrimaryHireContact", {}).get("emailaddress1", "No Contact Email") if parent_contact  else None
    _new_primarytrainingcontact_value = account.get("new_PrimaryTrainingContact", {}).get("emailaddress1", "No Account Number") if parent_account  else None

    """Map account fields from CRM to MoEngage."""
    attributes= {
        "Account Number": account.get("accountnumber"),
        "u_em": account.get("emailaddress1"),
        "u_mb": account.get("telephone1"),
        "Account Name": account.get("name"),
        "Created On": account.get("createdon"),
        "Modified On": account.get("modifiedon"),
        "new_afiUpliftemail": account.get("new_afiupliftemail"),
        "new_underbridgevanmountemail": account.get("new_underbridgevanmountemail"),
        "Rapid Email": account.get("new_rapidemail"),
        "Rentals Special Offers": account.get("new_rentalsspecialoffers"),
        "Rsale Email": account.get("new_resaleemail"),
        "Track Email": account.get("new_trackemail"),
        "Truck Email": account.get("new_truckemail"),
        "UTN Email": account.get("new_utnemail"),
        "Hoists Email": account.get("new_hoistsemail"),
        "address1_city": account.get("address1_city"),
        "SIC Code": account.get("sic"),
        "Company Registration No": account.get("new_registration_no"),
        # "Parent Contact Email": parent_contact_email,  # Parent contact email
        # "Parent Account Number": parent_account_number
        "Primary Hire Contact": _new_primaryhirecontact_value,
        "Last Invoice Date": account.get("new_lastinvoicedate"),
        "Last Training Date": account.get("new_lasttrainingdate"),
        "Group AM": account.get("new_groupaccountmanager"),
        "Rental AM": account.get("new_rentalam"),
        "donotphone": account.get("donotphone"),
        "donotemail": account.get("donotemail"),
        "Primary Training Contact": _new_primarytrainingcontact_value ,
        "address1_line1": account.get("address1_line1"),
        "address1_line2": account.get("address1_line2"),
        "address1_line3": account.get("address1_line3"),
        "Credit Limit": account.get("creditlimit"),
        "2 Years Ago Spent": account.get("new_twoyearsagorevenue"),
        "TPS Status": account.get("data8_tpsstatus"),
        "Credit Position": account.get("new_creditposition"),
        "Last Year Spent": account.get("new_lastyearrevenue"),
        "Account Status": account.get("statuscode"),
        "Postal Code": account.get("address1_postalcode"),
        "new_accountopened": account.get("new_accountopened"),
    }



    customer_id=attributes.get("u_em")

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
                "device_id": "96bd03b6-defc-4203-83d3-dc1c73080232",  # Replace with actual device ID if available
                "actions": []  # Empty actions array as per your example
            },
        ],
    }
    
    return final_payload





@router.get("/sync")
async def sync_accounts():
    """Fetch accounts from CRM and send them to MoEngage."""
    try:
        accounts_response = await fetch_accounts()
        accounts = accounts_response.get("accounts", [])

        headers = {
            'Authorization': token_moe,
            'Content-Type': 'application/json',
            'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C_DEBUG'
        }

        

        # Send accounts to MoEngage
        for account in accounts:
            payload = map_account_to_moengage(account)
            response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)
            if response.status_code == 200:
                print(f"Account {account['emailaddress1']} sent successfully")
            else:
                print(f"Failed to send account {account['emailaddress1']}: {response.text}")

        return {"status": "Accounts synchronized successfully"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
