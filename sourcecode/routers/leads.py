import requests
from fastapi import APIRouter, HTTPException,Query
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta
import boto3
import json,httpx

router = APIRouter()

# Initialize the AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")

S3_BUCKET_NAME = "apierrorlog"

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
    MOENGAGE_API_URL = secrets.get("MOENGAGE_API_URL", "")
    moe_token = secrets.get("moe_token", "")
else:
    raise HTTPException(status_code=500, detail="Failed to load secrets")

# Authorization token for MoEngage
token_moe = f"Basic {moe_token}"
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
    print(bucketname)
    print(log_records)

    try:
        log_timestamp=f"{key_prefix}{source}_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_log.json"
        s3 = boto3.client('s3')
        s3.put_object(Body=log_records, Bucket=bucketname, Key=log_timestamp)
       

        print(f"records pushed to aws s3 bucket://{bucketname}/{log_timestamp}")
                      
    except Exception as e:
        error_message=str(e)
        log_error(bucketname,error_message)
        raise HTTPException(status_code=500,details="Records count failed to log.")
    





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
        one_hour_ago = (datetime.utcnow() - timedelta(hours=1))

        # Format the DateTimeOffset correctly for CRM API (including UTC timezone)
        period = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # Exclude extra microseconds and add 'Z' for UTC

        print(f"Formatted time: {period}")

        # Set the API endpoint to fetch leads from Dynamics 365 CRM # top 5 is set up here for dev
        leads_url = f"{CRM_API_URL}/api/data/v9.0/leads?$filter=createdon ge {period}&$select=lastname,new_afileadscore,_parentcontactid_value,_parentaccountid_value,companyname,mobilephone,telephone1,emailaddress1,new_leadtype,leadsourcecode,new_utm_campaign,new_utm_campaignname,new_utm_content,new_utm_source,new_utm_medium,new_utm_term,new_utm_keyword,createdon,_ownerid_value,statuscode,subject&$expand=parentcontactid($select=emailaddress1),parentaccountid($select=accountnumber)"

        # Initialize an empty list to store leads
        all_leads = []

        # Fetch leads with pagination
        while leads_url:
            response = requests.get(leads_url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                all_leads.extend(data.get("value", []))  # Add the leads to the list

                leads_url = data.get("@odata.nextLink")
                
          
                if leads_url:
                    print(f"Fetching more leads from {leads_url}")
            else:
           
                error_message = f"Failed to fetch leads: {response.status_code} - {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch leads from CRM.")
        
        # Return the aggregated leads
        return {"leads": all_leads}
    
    except Exception as e:
        error_message = f"Error during fetch-leads: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


async def map_lead_to_moengage(lead):

    print(lead)
    

    try:

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
            "Owner": internal_email_address,
            "Topic": lead.get("subject"),  
            # "Modified On": account.get("modifiedon"),
            "Parent Contact Email": parent_contact_email,  # Parent contact email
            "Parent Account Number": parent_account_number  # Parent account number  

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
        raise HTTPException(status_code=500,details="failed in map function")


async def send_to_moengage(leads):
    success_count = 0
    fail_count = 0
    print(len(leads))

    headers = {
        'Authorization': token_moe,
        'Content-Type': 'application/json',
        'MOE-APPKEY': '6978DCU8W19J0XQOKS7NEE1C_DEBUG'
    }

    success_records = []
    failed_records = []

    try:
        for lead in leads:
            payload = await map_lead_to_moengage(lead)
          
            response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)

            if response.status_code == 200:
                success_count += 1
                record = {
                    "email": lead['emailaddress1'],
                    "status": response.text
                }
                success_records.append(record)
                print(f"Lead {lead['emailaddress1']} sent successfully")

            else:
                fail_count += 1
                record = {
                    "email": lead['emailaddress1'],
                    "error": response.text
                }


                failed_records.append(record)

                # send the falied payload to sqs
                await send_to_SQS(payload)

                print(f"Failed to send lead {lead['emailaddress1']}: {response.text}")
                

    except Exception as e:
        print(str(e))

    log_message = json.dumps({
        "timestamp": datetime.utcnow().isoformat(),
        "success_count": success_count,
        "fail_count": fail_count,
        "total_accounts": len(leads),
        "success_records": success_records,
        "failed_records": failed_records
    }, indent=4)  # Optional: indent makes JSON more readable

    log_processedRecords(S3_BUCKET_NAME, log_message)

@router.post("/SQS")  # Fixed route path
async def send_to_SQS(failed_payload: dict):  # Explicitly type `failed_payload` as a dictionary
    # Create a new SQS client
    sqs = boto3.client('sqs', region_name="eu-north-1")  # Specify the region explicitly if required
    queue_url = "https://sqs.eu-north-1.amazonaws.com/062314917923/TestRevanth"  # Replace with your SQS URL
#     payload= {'type': 'transition', 'elements': [{'type': 'customer', 'customer_id': 'derek@derekmcaleese.com', 'attributes': {'u_em': 'derek@derekmcaleese.com', 'u_mb': None, 
# 'telephone1': '01233 638996', 'Created On': '2019-07-06T06:53:46Z', 'Modified On': None, 'new_contacttype': None, '_accountid_value': None, '_parentcustomerid_value': 'DESTRA ENGINEERING LIMITED', 'jobtitle': 'Managing Director', 'u_fn': 'Derek', 'u_ln': 'Rawlings', 'address1_city': 'ASHFORD', 'address1_line1': 'Unit 5 St Georges Bus Ctr', 'address1_line2': 'Brunswick Rd Cobbs Wood', 'address1_line3': None, 'address1_postalcode': 'TN23 1EL', 'donotemail': False, 'donotphone': False, 'new_afiupliftemail': True, 'new_underbridgevanmountemail': None, 'new_rapidemail': True, 'new_rentalsspecialoffers': None, 'new_resaleemail': True, 'new_trackemail': None, 'new_truckemail': None, 'new_utnemail': True, 'new_hoistsemail': None, 'data8_tpsstatus': None, 'new_lastmewpscall': None, 'new_lastmewpscallwith': None, 
# 'new_lastemailed': None, 'new_lastemailedby': None, 'new_lastcalled': None, 'new_lastcalledby': None, 'new_registerforupliftonline': None, 'preferredcontactmethodcode': 1}}, {'type': 'event', 'customer_id': 'derek@derekmcaleese.com', 'actions': []}]}
#     failed_payload=payload
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




# Endpoint to fetch and send leads to MoEngage
@router.get("/sync-leads")
async def sync_leads():
    try:
        leads_response = await fetch_leads()
        leads = leads_response.get("leads", [])

        # Send leads to MoEngage
        await send_to_moengage(leads)


        return {"status": "Leads synchronized successfully to moengage"}

    except Exception as e:
        error_message = f"Error during sync-leads: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
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


@router.get("/retry-leads")
async def retry_failed_payloads_from_sqs():
    sqs = boto3.client('sqs')
    queue_url = "https://sqs.eu-north-1.amazonaws.com/062314917923/TestRevanth"

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
                    response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)

                    if response.status_code == 200:
                        print(f"Successfully retried payload: {payload}")
                        return {"your finished"}
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
# async def retry_failed_payloads_from_sqs():
#     sqs = boto3.client('sqs')
#     queue_url = "https://sqs.eu-north-1.amazonaws.com/062314917923/TestRevanth"  # Replace with your SQS URL

#     try:
#         while True:  
#             response = sqs.receive_message(
#                 QueueUrl=queue_url,
#                 MaxNumberOfMessages=10,  # Fetch up to 10 messages
#                 WaitTimeSeconds=10       # Long polling
#             )

#             # If no messages are found, break the loop
#             if 'Messages' not in response:
#                 print("No more messages to process.")
#                 break

#             for message in response['Messages']:
#                 try:
#                     # Parse the message body
#                     payload = json.loads(message['Body'])

#                     # Retry sending the payload to MoEngage
#                     headers = {
#                         'Authorization': token_moe,
#                         'Content-Type': 'application/json',
#                         'MOE-APPKEY': '6978DCU8W19J0XQOKS7NEE1C_DEBUG'
#                     }
#                     response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)

#                     if response.status_code == 200:
#                         print(f"Successfully retried payload: {payload}")
#                     else:
#                         print(f"Failed to retry payload: {payload}, Error: {response.text}")
#                         raise Exception(response.text)

#                     # Delete the message from the queue upon success
#                     sqs.delete_message(
#                         QueueUrl=queue_url,
#                         ReceiptHandle=message['ReceiptHandle']
#                     )
#                     print("Message deleted from SQS.")

#                 except Exception as e:
#                     print(f"Error processing message: {str(e)}")
#                     # Optionally, log the error and leave the message in SQS for another retry

#     except Exception as e:
#         error_message = f"Error while retrying failed payloads from SQS: {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500, detail=error_message)


