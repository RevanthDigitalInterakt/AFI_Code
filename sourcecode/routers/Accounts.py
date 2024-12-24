import requests
from fastapi import APIRouter, HTTPException
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta
import boto3,json,httpx



router = APIRouter()

# Initialize Boto3 client for Secrets Manager
secrets_client = boto3.client('secretsmanager')
S3_BUCKET_NAME = "crmtomoe"

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



def log_error(bucketname: str, error_log: str, source:str ="accounts",key_prefix: str = "errorlogs/"):
    try:
        log_time = f"{key_prefix}{source}_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_error.log"
        s3 = boto3.client('s3')
        s3.put_object(Body=error_log, Bucket=bucketname, Key=log_time)
        print(f"Error logged to S3://{bucketname}/{log_time}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to log in S3 bucket. S3: {str(e)}")

def log_processedRecords(bucketname:str,log_records:str,source:str ="accounts",key_prefix:str='processedRecords/'):
  
    try:
        log_timestamp=f"{key_prefix}{source}_{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}_log.json"
        s3 = boto3.client('s3')
        s3.put_object(Body=log_records, Bucket=bucketname, Key=log_timestamp)
       

        print(f"records pushed to aws s3 bucket://{bucketname}/{log_timestamp}")
                      
    except Exception as e:
        error_message = str(e)
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500,details="Processed Records Failed to log.")
    



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
       
        period = (datetime.utcnow() - timedelta(hours=1.5)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
       
        accounts_url = f"{CRM_API_URL}/api/data/v9.0/accounts?$filter=(createdon ge {period} or modifiedon ge {period})&$select={query}&$expand=new_PrimaryHireContact($select=emailaddress1),new_PrimaryTrainingContact($select=emailaddress1)"
        all_accounts = []

        async with httpx.AsyncClient() as client:
            while accounts_url:
                response = requests.get(accounts_url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    all_accounts.extend(data.get("value", []))
                    accounts_url = data.get("@odata.nextLink")
                else:
                    # error_message = f"Failed to fetch accounts: {response.status_code} - {response.text}"
                    error_message=f"failed to fetch accounts:{response.status_code} - {response.text}"
                    log_error(S3_BUCKET_NAME, error_message)
                    raise HTTPException(status_code=response.status_code, detail="Failed to fetch accounts from CRM.")

            return {"accounts": all_accounts}
        
    except httpx.RequestError as e:
        error_message=f"Error during HTTP Request:{str(e)}"
        log_error(S3_BUCKET_NAME,error_message)
        raise HTTPException(status_code=500,detail="Error during HTTP Request."
        )

    except Exception as e:
        error_message = f"Error during fetch-Accounts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


def map_account_to_moengage(account):

    """Map account fields from CRM to MoEngage."""
    try:



        primary_hire_account=account.get("new_PrimaryHireContact")
        primary_training_contact=account.get("new_PrimaryTrainingContact")

        primary_account_value=account.get("new_PrimaryHireContact",{}).get("emailaddress1","No Hire Value") if primary_hire_account else None
        primary_training_value=account.get("new_PrimaryTrainingContact",{}).get("emailaddress1","NO Training Value") if primary_training_contact else None
        
        attributes={
            "u_em": account.get("emailaddress1"),
            "u_mb": account.get("telephone1"),
            "Account Number": account.get("accountnumber", "") or "",
            "Account Name": account.get("name", "") or "",
            "account_Created On": account.get("createdon", "") or "",
            "account_Modified On": account.get("modifiedon", "") or "",
            "account_new_afiUpliftemail": account.get("new_afiupliftemail", "") or "",
            "account_new_underbridgevanmountemail": account.get("new_underbridgevanmountemail", "") or "",
            "account_Rapid Email": account.get("new_rapidemail", "") or "",
            "account_Rentals Special Offers": account.get("new_rentalsspecialoffers", "") or "",
            "account_Resale Email": account.get("new_resaleemail", "") or "",
            "account_Track Email": account.get("new_trackemail", "") or "",
            "account_Truck Email": account.get("new_truckemail", "") or "",
            "account_UTN Email": account.get("new_utnemail", "") or "",
            "account_Hoists Email": account.get("new_hoistsemail", "") or "",
            "account_address1_city": account.get("address1_city", "") or "",
            "account_SIC Code": account.get("sic", "") or "",
            "account_Company Registration No": account.get("new_registration_no", "") or "",
            "account_Primary Hire Contact": primary_account_value or "",
            "account_Last Invoice Date": account.get("new_lastinvoicedate", "") or "",
            "account_Last Training Date": account.get("new_lasttrainingdate", "") or "",
            "account_Group AM": account.get("new_groupaccountmanager", "") or "",
            "account_Rental AM": account.get("new_rentalam", "") or "",
            "account_donotphone": account.get("donotphone", "") or "",
            "account_donotemail": account.get("donotemail", "") or "",
            "account_Primary Training Contact": primary_training_value or "",
            "account_address1_line1": account.get("address1_line1", "") or "",
            "account_address1_line2": account.get("address1_line2", "") or "",
            "account_address1_line3": account.get("address1_line3", "") or "",
            "account_Credit Limit": account.get("creditlimit", "") or "",
            "account_2 Years Ago Spent": account.get("new_twoyearsagorevenue", "") or "",
            "account_TPS Status": account.get("data8_tpsstatus", "") or "",
            "account_Credit Position": account.get("new_creditposition", "") or "",
            "account_Last Year Spent": account.get("new_lastyearrevenue", "") or "",
            "account_Account Status": account.get("statuscode", "") or "",
            "account_Postal Code": account.get("address1_postalcode", "") or "",
            "account_new_accountopened": account.get("new_accountopened", "") or "",
            "account_YTD": account.get("new_ytd", "") or "",
            "account_data8_tpsstatus": account.get("data8_tpsstatus", "") or "",
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
                    "actions": []  # Empty actions array as per your example
                },
            ],
        }
        print(final_payload)
        
        return final_payload

    except Exception as e:
        error_message = f"Error during map-to-account function: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500,details="failed in map-to-account function")



@router.get("/sync")
async def sync_accounts():
    """Fetch accounts from CRM and send them to MoEngage."""
    
    try:
        print("entered sync accounts")
        accounts_response = await fetch_accounts()
        accounts = accounts_response.get("accounts", [])

        await send_to_moengage(accounts)

        
        return {"status": "Accounts synchronized to moengage.Please verify the status"}


    except Exception as e:
        error_message = f"Error during sync-Accounts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")




async def send_to_moengage(accounts):

    success_count=0
    fail_count=0

    success_records=[]
    failed_records=[]

    headers = {
        'Authorization': token_moe,
        'Content-Type': 'application/json',
        'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C'
    }

    try:      
       
        # Send accounts to MoEngage
        for account in accounts:
            payload = map_account_to_moengage(account)
            try:
                response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)
                if response.status_code == 200:
                    success_count+=1
                    record = {
                        "email": account['emailaddress1'],
                        "status": response.text
                    }
                    success_records.append(record)
                    print(success_records)
                    print(f"Account {account['emailaddress1']} sent successfully")
                else:
                    fail_count += 1
                    record = {
                        "email": account['emailaddress1'],
                        "status": response.text
                    }
                    failed_records.append(record)
                    await send_to_SQS(payload)
                    print(failed_records)
                    error_message = f"Failed to send account {account['emailaddress1']}: {response.text}"
                    log_error(S3_BUCKET_NAME, error_message)  # Log the error
                    print(f"Account {account['emailaddress1']} failed with error: {response.text}")
                  
                  
                    # print(f"Failed to send account {account['emailaddress1']}: {response.text}")
                    # fail_count+=1
                    # record = {
                    #     "email": account['emailaddress1'],
                    #     "status": response.text
                    # }
                    # failed_records.append(record)
                    # await send_to_SQS(payload)
                    # print(failed_records)
                    # print(f"Account {account['emailaddress1']} sent successfully")

            except Exception as e:
                print(e)
                error_message=f"Error Occured while sending the payload to moengage:{str(e)}"
                log_error(S3_BUCKET_NAME, error_message)
                raise HTTPException(status_code=500,details=f"{str(e)}")

        log_message = json.dumps({
                "timestamp": datetime.utcnow().isoformat(),
                "success_count": success_count,
                "fail_count": fail_count,
                "total_accounts": len(accounts),
                "success_records": success_records,
                "failed_records": failed_records
            }, indent=4)
        
        log_processedRecords(S3_BUCKET_NAME, log_message)

        return {"status": "Accounts synchronized successfully"}
    except Exception as e:
        error_message = f"Error while sending accounts : {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500,details=f"{str(e)}")
    


@router.post("/SQS")  # Fixed route path
async def send_to_SQS(failed_payload: dict):  # Explicitly type `failed_payload` as a dictionary
   
   
   
    # Create a new SQS client
    sqs = boto3.client('sqs', region_name="eu-north-1")  # Specify the region explicitly if required
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
        
        # Raise HTTPException for FastAPI error response
        raise HTTPException(status_code=500, detail=error_message)





