import requests
from fastapi import APIRouter, HTTPException
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta
import boto3
import json,httpx

router = APIRouter()

# Initialize the AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")
S3_BUCKET_NAME = "apierrorlog"

#log the errors
def log_error(bucketname: str, error_log: str, source:str ="contacts",key_prefix: str = "errorlogs/"):
    try:
        log_time = f"{key_prefix}{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_error.log"
        s3 = boto3.client('s3')
        s3.put_object(Body=error_log, Bucket=bucketname, Key=log_time)
        print(f"Error logged to S3://{bucketname}/{log_time}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to log in S3 bucket. S3: {str(e)}")


#log the records
def log_processedRecords(bucketname:str,log_records:str,source:str ="contacts",key_prefix:str='processedRecords/'):
    print(bucketname)
    print(log_records)
    try:
        log_timestamp=f"{key_prefix}{source}_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_log.json"
        s3 = boto3.client('s3')
        s3.put_object(Body=log_records, Bucket=bucketname, Key=log_timestamp)
        # s3.upload_file()

        print(f"records pushed to aws s3 bucket://{bucketname}/{log_timestamp}")
                      
    except Exception as e:
        error_message = str(e)
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500,details="Processed Records failed to log.")
    

    


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

        query="emailaddress1,_accountid_value,_parentcustomerid_value,telephone1,mobilephone,jobtitle,firstname,address1_city,lastname,address1_line1,address1_line2,address1_line3,address1_postalcode,donotemail,donotphone,new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,data8_tpsstatus,new_lastmewpscall,new_lastmewpscallwith,new_lastemailed,new_lastemailedby,new_lastcalled,new_lastcalledby,new_registerforupliftonline,createdon,preferredcontactmethodcode"       
        
        period = (datetime.utcnow() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        contacts_url = f"{CRM_API_URL}/api/data/v9.0/contacts?$filter=(createdon ge {period} or modifiedon ge {period})&$select={query}&$expand=parentcustomerid_account($select=accountnumber),parentcustomerid_account($select=name)"
        all_contacts = []
        print("just entered contacts")
        while contacts_url:
            response = requests.get(contacts_url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                all_contacts.extend(data.get("value", []))
                contacts_url = data.get("@odata.nextLink")
            else:
                error_message = f"Error while fetching-contacts: {str(e)}"
                log_error(S3_BUCKET_NAME, error_message)
                raise HTTPException(status_code=response.status_code, detail="Failed to fetch contacts from CRM.")
       
        return {"contacts": all_contacts}

    except Exception as e:
        error_message = f"Failed to fetch-contacts: {response.status_code} - {response.text}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


def map_contact_to_moengage(contact):
   
    parent_contact=contact.get("parentcustomerid_account")
    accountid_value = contact.get("parentcustomerid_account", {}).get("accountnumber", "No Account Number") if parent_contact  else None
    parentcustomerid_value = contact.get("parentcustomerid_account", {}).get("name", "No Account Name") if parent_contact  else None
    

    attributes= {
        # "u_n": contact.get("fullname"),
        "u_em": contact.get("emailaddress1"),
        "u_mb": contact.get("mobilephone"),
        "telephone1": contact.get("telephone1"),
        "Created On": contact.get("createdon"),
        "Modified On": contact.get("modifiedon"),
        "new_contacttype": contact.get("new_contacttype"),
        "_accountid_value": accountid_value,
        "_parentcustomerid_value": parentcustomerid_value,
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



@router.post("/SQS")  # Fixed route path
async def send_to_SQS(failed_payload: dict):  # Explicitly type `failed_payload` as a dictionary
    # Create a new SQS client
    sqs = boto3.client('sqs', region_name="eu-north-1")  # Specify the region explicitly if required
    queue_url = "https://sqs.eu-north-1.amazonaws.com/062314917923/TestRevanth"  


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



@router.get("/retry-contacts")
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
