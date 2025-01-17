import requests
from fastapi import APIRouter, HTTPException, Query
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta, timezone
import boto3
import json,httpx
import logging

router = APIRouter()

# Initialize the AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")
S3_BUCKET_NAME = "crmtomoetestattributes"

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
    MOENGAGE_API_URL_Test = secrets.get("MOENGAGE_API_URL_Test", "")
    moe_token = secrets.get("moe_token_test", "")
else:
    error_message=f"Check Scerets Manager Values"
    log_error(S3_BUCKET_NAME,error_message)
    raise HTTPException(status_code=500, detail="Failed to load secrets")


# Authorization token for MoEngage
token_moe = f"Basic {moe_token}"
global_token=None


@router.get("/fetch")
async def fetch_contacts():

    global global_token
    """Fetch contacts from Dynamics 365 CRM using the access token."""
    try:
        token = await authenticate_crm()
        global_token=token
        if not token:
            raise HTTPException(status_code=401, detail="Failed to retrieve access token")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        query = (
            "emailaddress1,_accountid_value,_parentcustomerid_value,modifiedon,telephone1,mobilephone,jobtitle," 
            "firstname,address1_city,lastname,address1_line1,address1_line2,address1_line3,address1_postalcode," 
            "donotemail,donotphone,new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers," 
            "new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,data8_tpsstatus,new_lastmewpscall," 
            "new_lastmewpscallwith,new_lastemailed,new_lastemailedby,new_lastcalled,new_lastcalledby,new_registerforupliftonline," 
            "createdon,preferredcontactmethodcode,statecode"
        )

        query2 = (
            "$expand=parentcustomerid_account($select=accountnumber,name,new_accountopened,creditlimit," 
            "new_creditposition,new_ytdrevenue,new_lastyearrevenue,new_twoyearsagorevenue,data8_tpsstatus," 
            "address1_line1,address1_line2,address1_line3,address1_city,address1_postalcode,sic,new_registration_no," 
            "_new_primaryhirecontact_value,_new_primarytrainingcontact_value,new_lastinvoicedate,new_lasttrainingdate," 
            "new_groupaccountmanager,new_rentalam,donotemail,donotphone,new_afiupliftemail,new_underbridgevanmountemail," 
            "new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail," 
            "new_hoistsemail,emailaddress1;$expand=new_PrimaryHireContact($select=emailaddress1),new_PrimaryTrainingContact($select=emailaddress1))"
        )

        
        one_hour_ago = (datetime.utcnow() - timedelta(hours=1))

        # Format the DateTimeOffset correctly for CRM API (including UTC timezone)
        period = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # Exclude extra microseconds and add 'Z' for UTC

        print(f"Formatted time: {period}")

        created_url = (f"{CRM_API_URL}/api/data/v9.0/contacts?"
                       f"$filter=(createdon ge {period})&$select={query}&{query2}")

        modified_url = (f"{CRM_API_URL}/api/data/v9.0/contacts?"
                        f"$filter=(modifiedon ge {period})&$select={query}&{query2}")

        # ist = timezone(timedelta(hours=5, minutes=30))
        # start_of_day_ist = datetime(2025, 1, 15, 0, 0, 0, tzinfo=ist)
        # end_of_day_ist = datetime(2025, 1, 15, 23, 59, 59, tzinfo=ist)

        # # Convert IST to UTC for the API query
        # start_period = start_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        # end_period = end_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'


        # created_url = (f"{CRM_API_URL}/api/data/v9.0/contacts?"
        #                f"$filter=(createdon ge {start_period} and createdon le {end_period})&$select={query}&{query2}")

        # modified_url = (f"{CRM_API_URL}/api/data/v9.0/contacts?"
        #                 f"$filter=(modifiedon ge {start_period} and modifiedon le {end_period})&$select={query}&{query2}")


        all_contacts = []
        created_on_contacts = []
        modified_on_contacts = []
        created_on_count = 0
        modified_on_count = 0

        async with httpx.AsyncClient() as client:
            for url, target_list, counter_key in [
                (created_url, created_on_contacts, 'created_on_count'),
                (modified_url, modified_on_contacts, 'modified_on_count')
            ]:
                while url:
                    response = await client.get(url, headers=headers)
                    if response.status_code == 200:
                        data = response.json()
                        new_records = [record for record in data.get("value", []) if record not in target_list]
                        target_list.extend(new_records)
                        all_contacts.extend(new_records)

                        if counter_key == 'created_on_count':
                            created_on_count += len(new_records)
                        else:
                            modified_on_count += len(new_records)

                        # Handle pagination for more records
                        url = data.get("@odata.nextLink")
                    else:
                        error_message = f"Error while fetching contacts: {response.text}"
                        log_error(S3_BUCKET_NAME, error_message)
                        raise HTTPException(status_code=response.status_code, detail="Failed to fetch contacts from CRM.")

        # Print total counts of created and modified records
        print(f"Total records fetched: {len(all_contacts)}")
        print(f"Created on records count: {created_on_count}")
        print(f"Modified on records count: {modified_on_count}")

       

        return {
            "contacts": all_contacts,
            "created_on_contacts": created_on_contacts,
            "modified_on_contacts": modified_on_contacts,
            "created_on_count": created_on_count,
            "modified_on_count": modified_on_count
        }

    except httpx.RequestError as e:
        error_message = f"Error during HTTP request: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail="Error during HTTP request.")
    except Exception as e:
        error_message = f"Failed to fetch contacts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")






async def map_contact_to_moengage(contact):
    print("entered map function")
    try:

        print("statecode")
        statecode_metadata_response = await fetch_statecode_metadata("statecode")

        print("statecode_metadata_response")
        print(statecode_metadata_response)
        statecode_options = {
            option["state"]: option["label"]
            for option in statecode_metadata_response["options"]
        }
        
        print("statecode_options")
        print(statecode_options)

        print("statecode")
        statecode = contact.get("statecode", None)
        print("statecode")
        print(statecode)
        statecode_type = statecode_options.get(statecode, "")
        print("statecode_type")
        print(statecode_type)

        
        # Extract parent account details
        parent_contact = contact.get("parentcustomerid_account") 
        accountid_value = parent_contact.get("accountnumber", "No Account Number") if parent_contact else None
        parentcustomerid_value = parent_contact.get("name", "No Account Name") if parent_contact else None
        primary_account_value = parent_contact.get("_new_primaryhirecontact_value", "") if parent_contact else None
        primary_training_value = parent_contact.get("_new_primarytrainingcontact_value", "") if parent_contact else None
        
        attributes = {
            "u_em": contact.get("emailaddress1", "") or "",
            "u_mb": contact.get("mobilephone", "") or "",
            "contact_statecode": statecode_type if statecode_type else "",
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
            "new_UTNEmail": contact.get("new_utnemail", "") or "",
            "new_trackemail": contact.get("new_trackemail", "") or "",
            "new_HoistEMail": contact.get("new_hoistsemail", "") or "",
            "new_underbridgevanmountemail": contact.get("new_underbridgevanmountemail", "") or "",
            "new_resaleemail":  contact.get("new_resaleemail", "") or "",
            "new_RentalsSpecialOffers": contact.get("new_rentalsspecialoffers", "") or "",
            "new_truckemail": contact.get("new_truckemail", "") or "",
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
        print("entered sync contacts")
        
        # Fetch the contacts from CRM (already filtered by created and modified dates)
        contacts_response = await fetch_contacts()
        
        # Extract the contacts directly from the response
        all_contacts = contacts_response.get("contacts", [])
        created_on_contacts = contacts_response.get("created_on_contacts", [])
        modified_on_contacts = contacts_response.get("modified_on_contacts", [])

        # Send the contacts to MoEngage with the necessary categorization
        await send_to_moengage(all_contacts, created_on_contacts, modified_on_contacts)
        
        return {"status": "Contacts synchronized successfully to MoEngage"}
       
    except Exception as e:
        error_message = f"Error during sync-contacts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



async def send_to_moengage(all_contacts, created_on_contacts, modified_on_contacts):
    # Initialize success and failure counts for each category
    success_all = 0
    fail_all = 0
    success_created = 0
    fail_created = 0
    success_modified = 0
    fail_modified = 0

    # Initialize lists to store success and failed records
    success_records_all = []
    failed_records_all = []
    success_records_created = []
    failed_records_created = []
    success_records_modified = []
    failed_records_modified = []

    print("MOE Token")
    print(token_moe)

    headers = {
        'Authorization': token_moe,
        'Content-Type': 'application/json',
        'MOE-APPKEY': '6978DCU8W19J0XQOKS7NEE1C_DEBUG'
    }

    # Process All Contacts
    for contact in all_contacts:
        email = contact.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_all += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_all.append(record)
            print(f"Account {email} has no valid email address")
            continue

        payload = await map_contact_to_moengage(contact)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                print(f"Contact sent successfully for {contact['emailaddress1']}")
                success_all += 1
                record = {"email": contact['emailaddress1'], "status": response.text}
                success_records_all.append(record)
            else:
                fail_all += 1
                record = {"email": contact['emailaddress1'], "status": response.text}
                failed_records_all.append(record)
                await send_to_SQS(payload)
                print(f"Failed to send contact {contact['emailaddress1']}: {response.text}")
                error_message = f"Failed to send account {contact['emailaddress1']}: {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
        except Exception as e:
            error_message = f"Error Occurred while sending the payload to MoEngage: {str(e)}"
            log_error(S3_BUCKET_NAME, error_message)
            print(e)
            raise HTTPException(status_code=500, detail=f"{str(e)}")

    # Process Created On Contacts
    for contact in created_on_contacts:
        email = contact.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_created += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_created.append(record)
            continue

        payload = await map_contact_to_moengage(contact)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                success_created += 1
                record = {"email": contact['emailaddress1'], "status": response.text}
                success_records_created.append(record)
            else:
                fail_created += 1
                record = {"email": contact['emailaddress1'], "status": response.text}
                failed_records_created.append(record)
                await send_to_SQS(payload)
                error_message = f"Failed to send account {contact['emailaddress1']}: {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
        except Exception as e:
            error_message = f"Error Occurred while sending the payload to MoEngage: {str(e)}"
            log_error(S3_BUCKET_NAME, error_message)
            print(e)
            raise HTTPException(status_code=500, detail=f"{str(e)}")

    # Process Modified On Contacts
    for contact in modified_on_contacts:
        email = contact.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_modified += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_modified.append(record)
            continue

        payload = await map_contact_to_moengage(contact)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                success_modified += 1
                record = {"email": contact['emailaddress1'], "status": response.text}
                success_records_modified.append(record)
            else:
                fail_modified += 1
                record = {"email": contact['emailaddress1'], "status": response.text}
                failed_records_modified.append(record)
                await send_to_SQS(payload)
                error_message = f"Failed to send account {contact['emailaddress1']}: {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
        except Exception as e:
            error_message = f"Error Occurred while sending the payload to MoEngage: {str(e)}"
            log_error(S3_BUCKET_NAME, error_message)
            print(e)
            raise HTTPException(status_code=500, detail=f"{str(e)}")

    # Log the processed records for each category
    log_message = json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "success_all": success_all,
        "fail_all": fail_all,
        "success_created": success_created,
        "fail_created": fail_created,
        "success_modified": success_modified,
        "fail_modified": fail_modified,
        "total_contacts": {
            "all": len(all_contacts),
            "created": len(created_on_contacts),
            "modified": len(modified_on_contacts)
        },
        "success_records_all": success_records_all,
        "failed_records_all": failed_records_all,
        "success_records_created": success_records_created,
        "failed_records_created": failed_records_created,
        "success_records_modified": success_records_modified,
        "failed_records_modified": failed_records_modified
    }, indent=4)

    log_processedRecords(S3_BUCKET_NAME, log_message)



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


async def fetch_statecode_metadata(attribute: str = Query("statecode", description="Logical name of the attribute to fetch metadata for")):
    global global_token

    token = global_token
    print("token:", token)  # Debug: print the token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Updated URL for fetching statecode metadata
    metadata_url = f"https://afi-group.crm11.dynamics.com/api/data/v9.2/EntityDefinitions(LogicalName='contact')/Attributes/Microsoft.Dynamics.CRM.StatusAttributeMetadata?$expand=OptionSet"
    print("Metadata URL:", metadata_url)  # Debug: print the URL being requested

    try:
        # Fetch metadata
        response = httpx.get(metadata_url, headers=headers)
        response.raise_for_status()

        # Extract the JSON response
        data = response.json()

        print("Response Data:")
        print(data)  # Debug: print the full response data

        # Check if OptionSet exists and if it contains Options
        option_set = data.get('value', [])[0].get('OptionSet', {})
        if not option_set or 'Options' not in option_set or not option_set['Options']:
            print("OptionSet not found or empty.")  # Debug: if OptionSet is missing or empty
            raise HTTPException(status_code=404, detail="OptionSet not found or empty in the response")

        # Debug: Check if 'Options' are present
        options_data = option_set['Options']
        print("Options Data:", options_data)  # Debug: print the options data

        # Extract options with State and Label
        options = [
            {
                "state": option.get("State"),
                "label": option.get("Label", {}).get("UserLocalizedLabel", {}).get("Label", "No label found"),
            }
            for option in options_data
        ]

        print("Extracted Options:")
        print(options)  # Debug: print the extracted options list

        # If the attribute is found, return the options
        return {
            "attribute": attribute,
            "options": options,
        }

    except httpx.HTTPStatusError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.text}")  # Debug: log the error response
        raise HTTPException(status_code=e.response.status_code, detail=f"HTTP Error: {e.response.text}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")  # Debug: log any other errors
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching statecode metadata: {str(e)}")

