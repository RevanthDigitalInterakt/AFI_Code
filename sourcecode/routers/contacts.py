import requests
from fastapi import APIRouter, HTTPException
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta
import boto3
import json,httpx
import logging


router = APIRouter()

# Initialize the AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")
S3_BUCKET_NAME = "crmtomoe"



# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def log_error(bucketname: str, error_log: str, source: str = "contacts", key_prefix: str = "errorlogs/"):
    try:
        log_time = f"{key_prefix}{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_error.log"
        s3 = boto3.client('s3')
        logging.info(f"Attempting to log error to S3 bucket: {bucketname}, key: {log_time}")
        s3.put_object(Body=error_log, Bucket=bucketname, Key=log_time)
        logging.info(f"Error logged to S3://{bucketname}/{log_time}")
    except Exception as e:
        logging.error(f"Failed to log error in S3 bucket. S3: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to log in S3 bucket. S3: {str(e)}")


def log_processedRecords(bucketname: str, log_records: str, source: str = "contacts", key_prefix: str = "processedRecords/"):
    try:
        log_timestamp = f"{key_prefix}{source}_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_log.json"
        s3 = boto3.client('s3')
        logging.info(f"Attempting to log processed records to S3 bucket: {bucketname}, key: {log_timestamp}")
        s3.put_object(Body=log_records, Bucket=bucketname, Key=log_timestamp)
        logging.info(f"Records logged to S3://{bucketname}/{log_timestamp}")
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error while logging processed records: {error_message}")
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail="Processed Records failed to log.")




# function for Client secrets

def get_secret(secret_name: str):
    """Fetch secrets from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        elif "SecretBinary" in response:
            return json.loads(response["SecretBinary"])
    except Exception as e:
        error_message=str(e)
        log_error(S3_BUCKET_NAME,error_message)
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
    error_message=f"Check Scerets Manager Values"
    log_error(S3_BUCKET_NAME,error_message)
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

        query="emailaddress1,_accountid_value,_parentcustomerid_value,modifiedon,telephone1,mobilephone,jobtitle,firstname,address1_city,lastname,address1_line1,address1_line2,address1_line3,address1_postalcode,donotemail,donotphone,new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,data8_tpsstatus,new_lastmewpscall,new_lastmewpscallwith,new_lastemailed,new_lastemailedby,new_lastcalled,new_lastcalledby,new_registerforupliftonline,createdon,preferredcontactmethodcode"       
        query2="$expand=parentcustomerid_account($select=accountnumber,name,new_accountopened,creditlimit,new_creditposition,new_ytdrevenue,new_lastyearrevenue,new_twoyearsagorevenue,data8_tpsstatus,address1_line1,address1_line2,address1_line3,address1_city,address1_postalcode,sic,new_registration_no,_new_primaryhirecontact_value,_new_primarytrainingcontact_value,new_lastinvoicedate,new_lasttrainingdate,new_groupaccountmanager,new_rentalam,donotemail,donotphone,new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,emailaddress1;$expand=new_PrimaryHireContact($select=emailaddress1),new_PrimaryTrainingContact($select=emailaddress1))"
        period = (datetime.utcnow() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        contacts_url = f"{CRM_API_URL}/api/data/v9.0/contacts?$filter=(createdon ge {period} or modifiedon ge {period})&$select={query}&{query2}"
        all_contacts = []
        print("just entered contacts")
        
        async with httpx.AsyncClient() as client:
            while contacts_url:
                response = await client.get(contacts_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_contacts.extend(data.get("value", []))
                    contacts_url = data.get("@odata.nextLink")
                else:
                    error_message = f"Error while fetching-contacts: {response.text}"
                    log_error(S3_BUCKET_NAME, error_message)
                    raise HTTPException(status_code=response.status_code, detail="Failed to fetch contacts from CRM.")

        return {"contacts": all_contacts}

    except httpx.RequestError as e:
        error_message = f"Error during HTTP request: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail="Error during HTTP request.")
    except Exception as e:
        error_message = f"Failed to fetch-contacts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



def map_contact_to_moengage(contact):
    print("entered map function")
    try:
        # Extract parent account details
        parent_contact = contact.get("parentcustomerid_account") 
        accountid_value = parent_contact.get("accountnumber", "No Account Number") if parent_contact else None
        parentcustomerid_value = parent_contact.get("name", "No Account Name") if parent_contact else None
        primary_account_value = parent_contact.get("_new_primaryhirecontact_value", "") if parent_contact else None
        primary_training_value = parent_contact.get("_new_primarytrainingcontact_value", "") if parent_contact else None
        
        attributes = {
            "u_em": contact.get("emailaddress1", "") or "",
            "u_mb": contact.get("mobilephone", "") or "",
            "contact_telephone1": contact.get("telephone1", "") or "",
            "contact_Created On": contact.get("createdon", "") or "",
            "contact_Modified On": contact.get("modifiedon", "") or "",
            "contact_new_contacttype": contact.get("new_contacttype", "") or "",
            "contact__accountid_value": accountid_value or "",
            "contact__parentcustomerid_value": parentcustomerid_value or "",
            "contact_jobtitle": contact.get("jobtitle", "") or "",
            "u_fn": contact.get("firstname", "") or "",
            "u_ln": contact.get("lastname", "") or "",
            "contact_address1_city": contact.get("address1_city", "") or "",
            "contact_address1_line1": contact.get("address1_line1", "") or "",
            "contact_address1_line2": contact.get("address1_line2", "") or "",
            "contact_address1_line3": contact.get("address1_line3", "") or "",
            "contact_address1_postalcode": contact.get("address1_postalcode", "") or "",
            "contact_donotemail": contact.get("donotemail", "") or "",
            "contact_donotphone": contact.get("donotphone", "") or "",
            "contact_new_afiupliftemail": contact.get("new_afiupliftemail", "") or "",
            "contact_new_underbridgevanmountemail": contact.get("new_underbridgevanmountemail", "") or "",
            "contact_new_rapidemail": contact.get("new_rapidemail", "") or "",
            "contact_new_rentalsspecialoffers": contact.get("new_rentalsspecialoffers", "") or "",
            "contact_new_resaleemail": contact.get("new_resaleemail", "") or "",
            "contact_new_trackemail": contact.get("new_trackemail", "") or "",
            "contact_new_truckemail": contact.get("new_truckemail", "") or "",
            "contact_new_utnemail": contact.get("new_utnemail", "") or "",
            "contact_new_hoistsemail": contact.get("new_hoistsemail", "") or "",
            "contact_data8_tpsstatus": contact.get("data8_tpsstatus", "") or "",
            "contact_new_lastmewpscall": contact.get("new_lastmewpscall", "") or "",
            "contact_new_lastmewpscallwith": contact.get("new_lastmewpscallwith", "") or "",
            "contact_new_lastemailed": contact.get("new_lastemailed", "") or "",
            "contact_new_lastemailedby": contact.get("new_lastemailedby", "") or "",
            "contact_new_lastcalled": contact.get("new_lastcalled", "") or "",
            "contact_new_lastcalledby": contact.get("new_lastcalledby", "") or "",
            "contact_new_registerforupliftonline": contact.get("new_registerforupliftonline", "") or "",
            "contact_preferredcontactmethodcode": contact.get("preferredcontactmethodcode", "") or "",
        }

        

        # Merge parent account details into the attributes mapping
        if parent_contact:
            attributes.update({
                "Account Email Address":parent_contact.get("emailaddress1", "") or "",
                "Account Number": parent_contact.get("accountnumber", "") or "",
                "Account_Mobile Number":parent_contact.get("mobilephone", "") or "",
                "Account Name": parent_contact.get("name", "") or "",
                "account_Created On": parent_contact.get("createdon", "") or "",
                "account_Modified On": parent_contact.get("modifiedon", "") or "",
                "account_new_afiUpliftemail": parent_contact.get("new_afiupliftemail", "") or "",
                "account_new_underbridgevanmountemail": parent_contact.get("new_underbridgevanmountemail", "") or "",
                "account_Rapid Email": parent_contact.get("new_rapidemail", "") or "",
                "account_Rentals Special Offers": parent_contact.get("new_rentalsspecialoffers", "") or "",
                "account_Resale Email": parent_contact.get("new_resaleemail", "") or "",
                "account_Track Email": parent_contact.get("new_trackemail", "") or "",
                "account_Truck Email": parent_contact.get("new_truckemail", "") or "",
                "account_UTN Email": parent_contact.get("new_utnemail", "") or "",
                "account_Hoists Email": parent_contact.get("new_hoistsemail", "") or "",
                "account_address1_city": parent_contact.get("address1_city", "") or "",
                "account_SIC Code": parent_contact.get("sic", "") or "",
                "account_Company Registration No": parent_contact.get("new_registration_no", "") or "",
                "account_Primary Hire Contact": primary_account_value or "",
                "account_Last Invoice Date": parent_contact.get("new_lastinvoicedate", "") or "",
                "account_Last Training Date": parent_contact.get("new_lasttrainingdate", "") or "",
                "account_Group AM": parent_contact.get("new_groupaccountmanager", "") or "",
                "account_Rental AM": parent_contact.get("new_rentalam", "") or "",
                "account_donotphone": parent_contact.get("donotphone", "") or "",
                "account_donotemail": parent_contact.get("donotemail", "") or "",
                "account_Primary Training Contact": primary_training_value or "",
                "account_address1_line1": parent_contact.get("address1_line1", "") or "",
                "account_address1_line2": parent_contact.get("address1_line2", "") or "",
                "account_address1_line3": parent_contact.get("address1_line3", "") or "",
                "account_Credit Limit": parent_contact.get("creditlimit", "") or "",
                "account_2 Years Ago Spent": parent_contact.get("new_twoyearsagorevenue", "") or "",
                "account_TPS Status": parent_contact.get("data8_tpsstatus", "") or "",
                "account_Credit Position": parent_contact.get("new_creditposition", "") or "",
                "account_Last Year Spent": parent_contact.get("new_lastyearrevenue", "") or "",
                "account_Account Status": parent_contact.get("statuscode", "") or "",
                "account_Postal Code": parent_contact.get("address1_postalcode", "") or "",
                "account_new_accountopened": parent_contact.get("new_accountopened", "") or "",
                "account_YTD": parent_contact.get("new_ytd", "") or "",
                "account_data8_tpsstatus": parent_contact.get("data8_tpsstatus", "") or "",
            })


        
        print(attributes)
        # Use email as the customer_id (unique identifier)
        customer_id = attributes.get("u_em")

        # Final payload structure
        final_payload = {
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

        return final_payload

    except Exception as e:
        error_message = f"error in map_contact_to_moengage in contacts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



    


@router.get("/sync")
async def sync_contacts():
    """Fetch contacts from CRM and send them to MoEngage."""
    
    try:
        print("enetered sync contacts")
        contacts_response = await fetch_contacts()
        contacts = contacts_response.get("contacts", [])
      
      
        await send_to_moengage(contacts)
        
        return {"status": "Contacts synchronized successfully to moengage"}
       
    except Exception as e:
        error_message = f"Error during sync-contacts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


async def send_to_moengage(contacts):
    success_count=0
    fail_count=0

    success_records=[]
    failed_records=[]
    print("printing token")
    print(moe_token)
    headers = {
        'Authorization': token_moe,
        'Content-Type': 'application/json',
        'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C_DEBUG'
    }

    # Send contacts to MoEngage
    try:
        for contact in contacts:
            payload = map_contact_to_moengage(contact)
            print("printing payload")
            print(payload)
            response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)
            if response.status_code == 200:
                print(f"Contact sent successfully for {contact['emailaddress1']} ")
                success_count+=1

                record = {
                    "email": contact['emailaddress1'],
                    "error": response.text
                }
                success_records.append(record)
            else:
                print(f"Failed to send contact {contact['emailaddress1']}: {response.text}")
                fail_count+=1
                record = {
                    "email": contact['emailaddress1'],
                    "error": response.text
                }
                failed_records.append(record)
                await send_to_SQS(payload)

        log_message = json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "success_count": success_count,
            "fail_count": fail_count,
            "total_accounts": len(contacts),
            "success_records": success_records,
            "failed_records": failed_records
        }, indent=4)  # Optional: indent makes JSON more readable

        log_processedRecords(S3_BUCKET_NAME, log_message)


    except Exception as e:
        error_message = f"Error during send-to-moengage function in contacts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500,details="Please contact the Developer")



@router.post("/SQS")  # Fixed route path
async def send_to_SQS(failed_payload: dict):  

    sqs = boto3.client('sqs', region_name="eu-north-1")  
    queue_url = "https://sqs.eu-north-1.amazonaws.com/062314917923/Payload_Queue"  


    payload= {'type': 'transition'}
    failed_payload=payload

    
    try:
        # Serialize and send the message to the SQS queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(failed_payload)  # Convert payload to JSON string
        )
        print(f"Failed payload sent to SQS: {response['MessageId']}")
        return {"message": "Payload successfully sent to SQS", "message_id": response['MessageId']}
    except Exception as e:
        # Log the error
        error_message = f"Error sending payload to SQS: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        
        # Raise HTTPException for FastAPI error response
        raise HTTPException(status_code=500, detail=error_message)



