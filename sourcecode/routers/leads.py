import requests
from fastapi import APIRouter, HTTPException,Query
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta, timezone
import boto3
import json,httpx

router = APIRouter()

# Initialize the AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")

S3_BUCKET_NAME = "crmtomoetestattributes"

def get_secret(secret_name: str):
    """Fetch secrets from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        elif "SecretBinary" in response:
            return json.loads(response["SecretBinary"])
    except Exception as e:
        error_message=f"Error fetching Secrets from AWS:{str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
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
    raise HTTPException(status_code=500, detail="Failed to load secrets")

# Authorization token for MoEngage
token_moe = f"Basic {moe_token}"
print(token_moe)
global_token=None





def log_error(bucketname:str,error_log:str,source:str ="leads",key_prefix:str ="errorlogs/"):
    
    try:
        log_time=f"{key_prefix}{source}{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_error.log"
        
        
        s3 = boto3.client('s3')
        s3.put_object(Body=error_log, Bucket=bucketname, Key=log_time)

        print(f"error logged to S3://{bucketname}/{log_time}")
    
    except Exception as e:
        raise HTTPException(f"failed to log in s3 bucket.S3:{str(e)}")
    





def log_processedRecords(bucketname:str,log_records:str,source:str ="leads",key_prefix:str='processedRecords/'):

    try:
        log_timestamp=f"{key_prefix}{source}_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_log.json"
        s3 = boto3.client('s3')
        s3.put_object(Body=log_records, Bucket=bucketname, Key=log_timestamp)
       

        print(f"records pushed to aws s3 bucket://{bucketname}/{log_timestamp}")
                      
    except Exception as e:
        error_message=str(e)
        log_error(bucketname,error_message)
        raise HTTPException(status_code=500,detail="Records count failed to log.")
    


@router.get("/fetch-leads")
async def fetch_leads():

    global global_token

    try:
        token = await authenticate_crm()
        global_token=token
        # global_token=token  # Ensure you're awaiting the async function
        if not token:
            raise HTTPException(status_code=401, detail="Failed to retrieve access token")
        # print(f"Token: {token}")
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        query = (
            "lastname,new_afileadscore,_parentcontactid_value,_parentaccountid_value,companyname,mobilephone,telephone1,emailaddress1,new_leadtype,leadsourcecode,new_utm_campaign,new_utm_campaignname,new_utm_content,new_utm_source,new_utm_medium,new_utm_term,new_utm_keyword,createdon,modifiedon,_ownerid_value,statuscode,subject,description"
        )

        query2 = (
            "$expand=parentcontactid($select=emailaddress1),parentaccountid($select=accountnumber)"
        )

                # Get the current time and subtract one hour to get the time range
        one_hour_ago = (datetime.utcnow() - timedelta(hours=1))

        # Format the DateTimeOffset correctly for CRM API (including UTC timezone)
        period = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # Exclude extra microseconds and add 'Z' for UTC

        print(f"Formatted time: {period}")
        created_url = (f"{CRM_API_URL}/api/data/v9.0/leads?"
                       f"$filter=(createdon ge {period} )&$select={query}&{query2}")

        modified_url = (f"{CRM_API_URL}/api/data/v9.0/leads?"
                        f"$filter=(modifiedon ge {period})&$select={query}&{query2}")
        

        #  # Define the date range in IST
        # ist = timezone(timedelta(hours=5, minutes=30))
        # start_of_day_ist = datetime(2025, 1, 15, 0, 0, 0, tzinfo=ist)
        # end_of_day_ist = datetime(2025, 1, 15, 23, 59, 59, tzinfo=ist)

        # # Convert IST to UTC for the API query
        # start_period = start_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        # end_period = end_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        # created_url = (f"{CRM_API_URL}/api/data/v9.0/leads?"
        #                f"$filter=(createdon ge {start_period} and createdon le {end_period})&$select={query}&{query2}")

        # modified_url = (f"{CRM_API_URL}/api/data/v9.0/leads?"
        #                 f"$filter=(modifiedon ge {start_period} and modifiedon le {end_period})&$select={query}&{query2}")

        
        
        
        
        all_leads = []
        created_on_leads = []
        modified_on_leads = []
        created_on_count = 0
        modified_on_count = 0

        async with httpx.AsyncClient() as client:
            for url, target_list, counter_key in [
                (created_url, created_on_leads, 'created_on_count'),
                (modified_url, modified_on_leads, 'modified_on_count')
            ]:
                while url:
                    response = await client.get(url, headers=headers)
                    if response.status_code == 200:
                        data = response.json()
                        new_records = [record for record in data.get("value", []) if record not in target_list]
                        target_list.extend(new_records)
                        all_leads.extend(new_records)

                        if counter_key == 'created_on_count':
                            created_on_count += len(new_records)
                        else:
                            modified_on_count += len(new_records)

                        # Handle pagination for more records
                        url = data.get("@odata.nextLink")
                    else:
                        error_message = f"Error while fetching accounts: {response.text}"
                        log_error(S3_BUCKET_NAME, error_message)
                        raise HTTPException(status_code=response.status_code, detail="Failed to fetch accounts from CRM.")

        # Print total counts of created and modified records
        print(f"Total records fetched: {len(all_leads)}")
        print(f"Created on records count: {created_on_count}")
        print(f"Modified on records count: {modified_on_count}")

        print("\n--- Fetched Leads ---")
        print("All Leads:")
  

        return {
            "leads": all_leads,
            "created_on_leads": created_on_leads,
            "modified_on_leads": modified_on_leads,
            "created_on_count": created_on_count,
            "modified_on_count": modified_on_count,
        }

    except httpx.RequestError as e:
        error_message = f"Error during HTTP request: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail="Error during HTTP request.")
    except Exception as e:
        error_message = f"Failed to fetch leads: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")





async def map_lead_to_moengage(lead):

    try:
        print("map-lead")
        new_leadtype_metadata_response = await fetch_metadata("new_leadtype")
        print("exit new_lead_metadata")
        new_leadtype_options = {
            option["value"]: option["label"]
            for option in new_leadtype_metadata_response["options"]
        }
        print("new lead type")
        print(new_leadtype_options)

        statuscode_metadata_response = await fetch_statuscode_metadata("statuscode")
        print("statuscode_metadata_response")
        print(statuscode_metadata_response)
        statuscode_options = {
            option["value"]: option["label"]
            for option in statuscode_metadata_response["options"]
        }
        print("statuscode_options")
        print(statuscode_options)

        leadsourcecode_metadata_response = await fetch_leadsourcecode_metadata("leadsourcecode")
        leadsourcecode_options = {
            option["value"]: option["label"]
            for option in leadsourcecode_metadata_response["options"]
        }

      
        print("lead values here")
        lead_type_value = lead.get("new_leadtype", None)
        print(lead_type_value)
        lead_type = new_leadtype_options.get(lead_type_value, "")
        print(lead_type)



        
        lead_source = leadsourcecode_options.get(lead.get("leadsourcecode"), "")
        print(lead_source)
        lead_status = statuscode_options.get(lead.get("statuscode"), "")
        print(lead_status)





        lead_status = statuscode_options.get(lead.get("statuscode"), "")
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
        owner_id=lead.get("_ownerid_value")
        email_data_response = await fetch_email_from_lead(owner_id)
        internal_email_address = email_data_response["internal_email_address"]
        print("check email here\n")
        print(internal_email_address)


        payload = {
                "leadid": lead.get("leadid", "") or "",
                "u_em": lead.get("emailaddress1", "") or "",  
                "u_mb": lead.get("mobilephone", "") or "",  
                "lead_telephone1": lead.get("telephone1", "") or "", 
                "lead_Company Name": lead.get("companyname", "") or "",  # Company name
                "lead_Lead Type": lead_type if lead_type else "",  # Use the mapped value
                "lead_Lead Source Code": lead_source if lead_source else "",  # Use the mapped value
                "lead_Status Code": lead_status if lead_status else "",  # Use the mapped value
                "lead_new_utm_campaign": lead.get("new_utm_campaign", "") or "",  # UTM Campaign
                "lead_new_utm_campaignname": lead.get("new_utm_campaignname", "") or "",  # UTM Campaign Name
                "lead_new_utm_content": lead.get("new_utm_content", "") or "",  # UTM Content
                "lead_new_utm_source": lead.get("new_utm_source", "") or "",  # UTM Source
                "lead_new_utm_medium": lead.get("new_utm_medium", "") or "",  # UTM Medium
                "lead_new_utm_term": lead.get("new_utm_term", "") or "",  # UTM Term
                "lead_new_utm_keyword": lead.get("new_utm_keyword", "") or "",  # UTM Keyword
                "lead_Created On": lead.get("createdon", "") or "",  # Created date
                "lead_Modified On": lead.get("modifiedon", "") or "",  # Modified date
                "lead_Owner": internal_email_address if internal_email_address else "",  # Owner
                "lead_Topic": lead.get("subject", "") or "",  # Topic
                "lead_description": lead.get("description", "") or "",
                "lead_Parent Contact Email": parent_contact_email if parent_contact_email else "",  # Parent contact email
                "lead_Parent Account Number": parent_account_number if parent_account_number else ""  # Parent account number
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
                    "actions": []  # Empty actions array as per your example
                },
            ],
        }
        print(final_payload)
        return final_payload
    
    except Exception as e:
        error_message = f"Error during map-to-lead function: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500,detail="failed in map function")


async def send_to_moengage(all_leads, created_on_leads, modified_on_leads):
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

    headers = {
        'Authorization': token_moe,
        'Content-Type': 'application/json',
        'MOE-APPKEY': '6978DCU8W19J0XQOKS7NEE1C_DEBUG'
    }

    # Process All Leads
    for lead in all_leads:
        email = lead.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_all += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_all.append(record)
            print(f"Lead {email} has no valid email address")
            continue

        payload = await map_lead_to_moengage(lead)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                print(f"Lead sent successfully for {lead['emailaddress1']}")
                success_all += 1
                record = {"email": lead['emailaddress1'], "status": response.text}
                success_records_all.append(record)
            else:
                fail_all += 1
                record = {"email": lead['emailaddress1'], "status": response.text}
                failed_records_all.append(record)
                await send_to_SQS(payload)
                print(f"Failed to send lead {lead['emailaddress1']}: {response.text}")
                error_message = f"Failed to send lead {lead['emailaddress1']}: {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
        except Exception as e:
            error_message = f"Error Occurred while sending the payload to MoEngage: {str(e)}"
            log_error(S3_BUCKET_NAME, error_message)
            print(e)
            raise HTTPException(status_code=500, detail=f"{str(e)}")

    # Process Created On Leads
    for lead in created_on_leads:
        email = lead.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_created += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_created.append(record)
            continue

        payload = await map_lead_to_moengage(lead)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                success_created += 1
                record = {"email": lead['emailaddress1'], "status": response.text}
                success_records_created.append(record)
            else:
                fail_created += 1
                record = {"email": lead['emailaddress1'], "status": response.text}
                failed_records_created.append(record)
                await send_to_SQS(payload)
                error_message = f"Failed to send lead {lead['emailaddress1']}: {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
        except Exception as e:
            error_message = f"Error Occurred while sending the payload to MoEngage: {str(e)}"
            log_error(S3_BUCKET_NAME, error_message)
            print(e)
            raise HTTPException(status_code=500, detail=f"{str(e)}")

    # Process Modified On Leads
    for lead in modified_on_leads:
        email = lead.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_modified += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_modified.append(record)
            continue

        payload = await map_lead_to_moengage(lead)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                success_modified += 1
                record = {"email": lead['emailaddress1'], "status": response.text}
                success_records_modified.append(record)
            else:
                fail_modified += 1
                record = {"email": lead['emailaddress1'], "status": response.text}
                failed_records_modified.append(record)
                await send_to_SQS(payload)
                error_message = f"Failed to send lead {lead['emailaddress1']}: {response.text}"
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
        "total_leads": {
            "all": len(all_leads),
            "created": len(created_on_leads),
            "modified": len(modified_on_leads)
        },
        "success_records_all": success_records_all,
        "failed_records_all": failed_records_all,
        "success_records_created": success_records_created,
        "failed_records_created": failed_records_created,
        "success_records_modified": success_records_modified,
        "failed_records_modified": failed_records_modified
    }, indent=4)

    log_processedRecords(S3_BUCKET_NAME, log_message)



    

@router.post("/SQS")  
async def send_to_SQS(failed_payload: dict):  
    # Create a new SQS client
    sqs = boto3.client('sqs', region_name="eu-north-1")  
    queue_url = "https://sqs.eu-north-1.amazonaws.com/062314917923/Payload_Queue"  
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
        
      
        raise HTTPException(status_code=500, detail=error_message)




# Endpoint to fetch and send leads to MoEngage
@router.get("/sync-leads")
async def sync_leads():
    """Fetch contacts from CRM and send them to MoEngage."""
    
    try:
        print("entered sync contacts")
        
        # Fetch the contacts from CRM (already filtered by created and modified dates)
        leads_response = await fetch_leads()

        print("leads_response")
        # print(leads_response)
        
        # Extract the contacts directly from the response
        all_leads = leads_response.get("leads", [])
        
        created_on_leads = leads_response.get("created_on_leads", [])
        
        modified_on_leads = leads_response.get("modified_on_leads", [])
        

        
        
        # Send the contacts to MoEngage with the necessary categorization
        await send_to_moengage(all_leads, created_on_leads, modified_on_leads)
        
        return {"status": "Contacts synchronized successfully to MoEngage"}
       
    except Exception as e:
        error_message = f"Error during sync-contacts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")




async def fetch_metadata(attribute: str = Query("new_leadtype", description="Logical name of the attribute to fetch metadata for")):
    
    global global_token

    # print(global_token)
    token = global_token
    print("printing globaltoken/n")
    # print(token)
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

        print("exit metadata")

        return {
            "attribute": attribute,
            "display_name": attribute_display_name,
            "options": options,
        }

    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=f"HTTP Error: {e.response.text}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching metadata: {str(e)}")



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




async def fetch_email_from_lead(owner_id:str):
    print("entered email fun")

    global global_token
    try:
        # Get the token dynamically
        token = global_token
        
        # Headers with the dynamic token
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }



        system_user_url = (
            f"{CRM_API_URL}/api/data/v9.0/systemusers"
            f"?$filter=systemuserid eq {owner_id}"
            "&$select=internalemailaddress,fullname"
        )
        print(system_user_url)
        system_user_response = requests.get(system_user_url, headers=headers)
        # print("System User Response:", system_user_response.status_code, system_user_response.text) 
        system_user_response.raise_for_status()

        system_user_data = system_user_response.json()
        print("printing email")
        print(system_user_data)
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


@router.get("/retry")
async def retry_failed_payloads_from_sqs():
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.eu-north-1.amazonaws.com/062314917923/Payload_Queue"

    try:
        while True:
            # Receive messages from SQS
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=10
            )

            # If no messages are found, break the loop
            if 'Messages' not in response:
                print("No more messages to process.")
                break

            for message in response['Messages']:
                try:
                    # Inspect the raw body before parsing
                    raw_body = message['Body']
                    print(f"Raw message body: {raw_body}")

                    # Attempt to parse the message body
                    try:
                        payload = json.loads(raw_body)
                    except json.JSONDecodeError as e:
                        print(f"Invalid JSON in message body: {raw_body}, Error: {str(e)}")
                        # Optionally, log the error and skip this message
                        continue

                    # Retry sending the payload to MoEngage
                    headers = {
                        'Authorization': token_moe,
                        'Content-Type': 'application/json',
                        'MOE-APPKEY': '6978DCU8W19J0XQOKS7NEE1C_DEBUG'
                    }
                    response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)

                    if response.status_code == 200:
                        print(f"Successfully retried payload: {payload}")
                        
                    else:
                        print(f"Failed to retry payload: {payload}, Error: {response.text}")
                        raise Exception(response.text)

                    # Delete the message from the queue upon success
                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    print("Message deleted from SQS.")

                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    # Optionally, log the error and leave the message in SQS for another retry

    except Exception as e:
        error_message = f"Error while retrying failed payloads from SQS: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=error_message)