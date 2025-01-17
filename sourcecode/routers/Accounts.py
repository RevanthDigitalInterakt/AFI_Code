import requests
from fastapi import APIRouter, HTTPException,Query
from sourcecode.crmAuthentication import authenticate_crm
from datetime import datetime, timedelta, timezone
import boto3,json,httpx

router = APIRouter()

# Initialize Boto3 client for Secrets Manager
secrets_client = boto3.client('secretsmanager')
S3_BUCKET_NAME = "crmtomoetestattributes"

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
    MOENGAGE_API_URL_Test = secrets.get("MOENGAGE_API_URL_Test", "default_value")
    moe_token = secrets.get("moe_token_test", "default_value")
else:
    print("Failed to load secrets.")
    CRM_API_URL = CRM_TOKEN_URL = CRM_CLIENT_ID = CRM_CLIENT_SECRET = MOENGAGE_API_URL_Test = moe_token = "default_value"

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
        raise HTTPException(status_code=500,detail="Processed Records Failed to log.")
    



@router.get("/fetch")
async def fetch_accounts():

    global global_token
    """Fetch accounts from Dynamics 365 CRM using the access token."""
    try:
        token = await authenticate_crm()
        global_token = token
        if not token:
            raise HTTPException(status_code=401, detail="Failed to retrieve access token")

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        # Specify the query and date range
        query = (
            "new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,"
            "new_resaleemail,new_resalerevenue,new_lastresaledate,new_resaleflagsage,new_smarevenue,new_faceliftemail,new_hseqemail,new_excel,new_sagecompany,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,address1_city,"
            "sic,new_registration_no,_new_primaryhirecontact_value,new_lastinvoicedate,new_lasttrainingdate,"
            "new_groupaccountmanager,new_rentalam,donotphone,donotemail,new_afiupliftemail,"
            "new_underbridgevanmountemail,_new_primarytrainingcontact_value,address1_line1,address1_line2,"
            "address1_line3,creditlimit,new_twoyearsagorevenue,data8_tpsstatus,new_creditposition,"
            "new_lastyearrevenue,statuscode,address1_postalcode,new_accountopened,name,"
            "_new_primaryhirecontact_value,accountnumber,telephone1,emailaddress1,createdon,modifiedon"
        )
        expand_query = (
            "$expand=new_PrimaryHireContact($select=emailaddress1),"
            "new_PrimaryTrainingContact($select=emailaddress1)"
        )
        ist = timezone(timedelta(hours=5, minutes=30))
        start_of_day_ist = datetime(2025, 1, 15, 0, 0, 0, tzinfo=ist)
        end_of_day_ist = datetime(2025, 1, 15, 23, 59, 59, tzinfo=ist)

        # Convert IST to UTC for the API query
        start_period = start_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        end_period = end_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        created_url = (
            f"{CRM_API_URL}/api/data/v9.0/accounts?"
            f"$filter=(createdon ge {start_period} and createdon le {end_period})"
            f"&$select={query}&{expand_query} "
        )
        modified_url = (
            f"{CRM_API_URL}/api/data/v9.0/accounts?"
            f"$filter=(modifiedon ge {start_period} and modifiedon le {end_period})"
            f"&$select={query}&{expand_query}"
        )

        # 
        
        # one_hour_ago = (datetime.utcnow() - timedelta(hours=1))

        # # Format the DateTimeOffset correctly for CRM API (including UTC timezone)
        # period = one_hour_ago.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'  # Exclude extra microseconds and add 'Z' for UTC

        # print(f"Formatted time: {period}")

        # # print(f"Fetching accounts from: {start_period} to {end_period}")

        # created_url = (
        #     f"{CRM_API_URL}/api/data/v9.0/accounts?"
        #     f"$filter=(createdon ge {period})"
        #     f"&$select={query}&{expand_query}"
        # )
        # modified_url = (
        #     f"{CRM_API_URL}/api/data/v9.0/accounts?"
        #     f"$filter=(modifiedon ge {period})"
        #     f"&$select={query}&{expand_query}"
        # )

        all_accounts = []
        created_on_accounts = []
        modified_on_accounts = []
        created_on_count = 0
        modified_on_count = 0

        async with httpx.AsyncClient() as client:
            for url, target_list, counter_key in [
                (created_url, created_on_accounts, 'created_on_count'),
                (modified_url, modified_on_accounts, 'modified_on_count')
            ]:
                while url:
                    response = await client.get(url, headers=headers)
                    if response.status_code == 200:
                        data = response.json()
                        new_records = [record for record in data.get("value", []) if record not in target_list]
                        target_list.extend(new_records)
                        all_accounts.extend(new_records)

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
        print(f"Total records fetched: {len(all_accounts)}")
        print(f"Created on records count: {created_on_count}")
        print(f"Modified on records count: {modified_on_count}")

        return {
            "accounts": all_accounts,
            "created_on_accounts": created_on_accounts,
            "modified_on_accounts": modified_on_accounts,
            "created_on_count": created_on_count,
            "modified_on_count": modified_on_count,
        }

    except httpx.RequestError as e:
        error_message = f"Error during HTTP request: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail="Error during HTTP request.")
    except Exception as e:
        error_message = f"Failed to fetch accounts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")




# async def fetch_accounts():
#     """Fetch accounts from Dynamics 365 CRM using the access token."""
#     try:
#         token = await authenticate_crm()
#         if not token:
#             raise HTTPException(status_code=401, detail="Failed to retrieve access token")

#         headers = {
#             "Authorization": f"Bearer {token}",
#             "Content-Type": "application/json",
#         }

#         # Specify the query and the date range
#         query = ("new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,"
#                  "new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,address1_city,"
#                  "sic,new_registration_no,_new_primaryhirecontact_value,new_lastinvoicedate,new_lasttrainingdate,"
#                  "new_groupaccountmanager,new_rentalam,donotphone,donotemail,new_afiupliftemail,"
#                  "new_underbridgevanmountemail,_new_primarytrainingcontact_value,address1_line1,address1_line2,"
#                  "address1_line3,creditlimit,new_twoyearsagorevenue,data8_tpsstatus,new_creditposition,"
#                  "new_lastyearrevenue,statuscode,address1_postalcode,new_accountopened,name,"
#                  "_new_primaryhirecontact_value,accountnumber,telephone1,emailaddress1,createdon,modifiedon")
        
#         # Define the date range in IST
#         ist = timezone(timedelta(hours=5, minutes=30))
#         start_of_day_ist = datetime(2025, 1, 7, 0, 0, 0, tzinfo=ist)
#         end_of_day_ist = datetime(2025, 1, 7, 23, 59, 59, tzinfo=ist)

#         # Convert IST to UTC for the API query
#         start_period = start_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
#         end_period = end_of_day_ist.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

#         # Initial API endpoint
#         accounts_url = (
#             f"{CRM_API_URL}/api/data/v9.0/accounts?"
#             f"$filter=(createdon ge {start_period} and createdon le {end_period} or "
#             f"modifiedon ge {start_period} and modifiedon le {end_period})&$select={query}&"
#             f"$expand=new_PrimaryHireContact($select=emailaddress1),new_PrimaryTrainingContact($select=emailaddress1)"
#         )
        
#         all_accounts = []
#         total_records_fetched = 0
#         created_on_count = 0
#         modified_on_count = 0

#         async with httpx.AsyncClient() as client:
#             while accounts_url:
#                 try:
#                     # Make the API request
#                     response = await client.get(accounts_url, headers=headers)
                    
#                     if response.status_code == 200:
#                         data = response.json()
#                         batch_records = data.get("value", [])
#                         all_accounts.extend(batch_records)
#                         total_records_fetched += len(batch_records)

#                         # Count records based on 'createdon' and 'modifiedon'
#                         for record in batch_records:
#                             if 'createdon' in record and record['createdon'] >= start_period and record['createdon'] <= end_period:
#                                 created_on_count += 1
#                                 print(f"Name: {record.get('name')}, Email: {record.get('emailaddress1')}, Account Number: {record.get('accountnumber')}")

#                             if 'modifiedon' in record and record['modifiedon'] >= start_period and record['modifiedon'] <= end_period:
#                                 modified_on_count += 1

#                         # Get the next page URL from @odata.nextLink, if it exists
#                         accounts_url = data.get("@odata.nextLink")
#                     else:
#                         # Enhanced error logging
#                         error_message = f"Failed to fetch accounts: {response.status_code} - {response.text}"
#                         log_error(S3_BUCKET_NAME, error_message)
#                         raise HTTPException(status_code=response.status_code, detail=f"Failed to fetch accounts: {response.status_code} - {response.text}")

#                 except httpx.RequestError as e:
#                     # Log full details for request errors
#                     error_message = f"Error during HTTP Request: {str(e)}"
#                     log_error(S3_BUCKET_NAME, error_message)
#                     raise HTTPException(status_code=500, detail=f"Error during HTTP Request: {str(e)}")
                
#                 except Exception as e:
#                     # General error handling
#                     error_message = f"Error during fetch-Accounts: {str(e)}"
#                     log_error(S3_BUCKET_NAME, error_message)
#                     raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

#         # Final logging of counts
#         print(f"Total records fetched: {total_records_fetched}")
#         print(f"Records created on: {created_on_count}")
#         print(f"Records modified on: {modified_on_count}")

#         return {"accounts": all_accounts, "created_on_count": created_on_count, "modified_on_count": modified_on_count}

#     except Exception as e:
#         # General exception logging and re-raise HTTP exception
#         error_message = f"Error in fetch_accounts function: {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500, detail=f"Error in fetch_accounts function: {str(e)}")

#     except Exception as e:
#         # General exception logging and re-raise HTTP exception
#         error_message = f"Error in fetch_accounts function: {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500, detail=f"Error in fetch_accounts function: {str(e)}")




# async def fetch_accounts():
#     """Fetch accounts from Dynamics 365 CRM using the access token."""
#     try:
#         token = await authenticate_crm()
#         if not token:
#             raise HTTPException(status_code=401, detail="Failed to retrieve access token")

#         headers = {
#             "Authorization": f"Bearer {token}",
#             "Content-Type": "application/json",
#         }

#         # Fetch accounts modified in the last 10 days
#         query="new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,address1_city,sic,new_registration_no,_new_primaryhirecontact_value,new_lastinvoicedate,new_lasttrainingdate,new_groupaccountmanager,new_rentalam,donotphone,donotemail,new_afiupliftemail,new_underbridgevanmountemail,_new_primarytrainingcontact_value,address1_line1,address1_line2,address1_line3,creditlimit,new_twoyearsagorevenue,data8_tpsstatus,new_creditposition,new_lastyearrevenue,statuscode,address1_postalcode,new_accountopened,name,_new_primaryhirecontact_value,accountnumber,telephone1,emailaddress1,createdon,modifiedon"
       
#         # period = (datetime.utcnow() - timedelta(hours=1.5)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
       
#         # accounts_url = f"{CRM_API_URL}/api/data/v9.0/accounts?$filter=(createdon ge {period} or modifiedon ge {period})&$select={query}&$expand=new_PrimaryHireContact($select=emailaddress1),new_PrimaryTrainingContact($select=emailaddress1)"
        
#         start_of_day = datetime(2025, 1, 3, 11, 59, 59)  # Start of the day
#         end_of_day = datetime(2025, 1, 3, 23, 59, 59)  # End of the day

# # Format the DateTimeOffset correctly for CRM API
#         start_period = start_of_day.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
#         end_period = end_of_day.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

#         print(f"Fetching accounts from: {start_period} to {end_period}")
       
#         accounts_url = f"{CRM_API_URL}/api/data/v9.0/accounts?$filter=(createdon ge {start_period} and createdon le {end_period} or modifiedon ge {start_period} and modifiedon le {end_period})&$select={query}&$expand=new_PrimaryHireContact($select=emailaddress1),new_PrimaryTrainingContact($select=emailaddress1)"
       
#         all_accounts = []

#         async with httpx.AsyncClient() as client:
#             while accounts_url:
#                 response = requests.get(accounts_url, headers=headers)
#                 if response.status_code == 200:
#                     data = response.json()
#                     all_accounts.extend(data.get("value", []))
#                     accounts_url = data.get("@odata.nextLink")
#                 else:
#                     # error_message = f"Failed to fetch accounts: {response.status_code} - {response.text}"
#                     error_message=f"failed to fetch accounts:{response.status_code} - {response.text}"
#                     log_error(S3_BUCKET_NAME, error_message)
#                     raise HTTPException(status_code=response.status_code, detail="Failed to fetch accounts from CRM.")

#             return {"accounts": all_accounts}
        
#     except httpx.RequestError as e:
#         error_message=f"Error during HTTP Request:{str(e)}"
#         log_error(S3_BUCKET_NAME,error_message)
#         raise HTTPException(status_code=500,detail="Error during HTTP Request."
#         )

#     except Exception as e:
#         error_message = f"Error during fetch-Accounts: {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
    


# async def fetch_accounts():
#     """Fetch accounts from Dynamics 365 CRM using the access token."""
#     try:
#         token = await authenticate_crm()
#         if not token:
#             raise HTTPException(status_code=401, detail="Failed to retrieve access token")

#         headers = {
#             "Authorization": f"Bearer {token}",
#             "Content-Type": "application/json",
#             'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C'
#         }

#         # Fetch accounts modified in the last 10 days
#         query="new_afiupliftemail,new_underbridgevanmountemail,new_rapidemail,new_rentalsspecialoffers,new_resaleemail,new_trackemail,new_truckemail,new_utnemail,new_hoistsemail,address1_city,sic,new_registration_no,_new_primaryhirecontact_value,new_lastinvoicedate,new_lasttrainingdate,new_groupaccountmanager,new_rentalam,donotphone,donotemail,new_afiupliftemail,new_underbridgevanmountemail,_new_primarytrainingcontact_value,address1_line1,address1_line2,address1_line3,creditlimit,new_twoyearsagorevenue,data8_tpsstatus,new_creditposition,new_lastyearrevenue,statuscode,address1_postalcode,new_accountopened,name,_new_primaryhirecontact_value,accountnumber,telephone1,emailaddress1,createdon,modifiedon"
       
#         period = (datetime.utcnow() - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
       
#         accounts_url = f"{CRM_API_URL}/api/data/v9.0/accounts?$filter=(createdon ge {period} or modifiedon ge {period})&$select={query}&$expand=new_PrimaryHireContact($select=emailaddress1),new_PrimaryTrainingContact($select=emailaddress1)"
#         all_accounts = []

#         async with httpx.AsyncClient() as client:
#             while accounts_url:
#                 response = requests.get(accounts_url, headers=headers)
#                 if response.status_code == 200:
#                     data = response.json()
#                     all_accounts.extend(data.get("value", []))
#                     accounts_url = data.get("@odata.nextLink")
#                 else:
#                     # error_message = f"Failed to fetch accounts: {response.status_code} - {response.text}"
#                     error_message=f"failed to fetch accounts:{response.status_code} - {response.text}"
#                     log_error(S3_BUCKET_NAME, error_message)
#                     raise HTTPException(status_code=response.status_code, detail="Failed to fetch accounts from CRM.")

#             return {"accounts": all_accounts}
        
#     except httpx.RequestError as e:
#         error_message=f"Error during HTTP Request:{str(e)}"
#         log_error(S3_BUCKET_NAME,error_message)
#         raise HTTPException(status_code=500,detail="Error during HTTP Request."
#         )

#     except Exception as e:
#         error_message = f"Error during fetch-Accounts: {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500, detail=f"Error: {str(e)}")



async def map_account_to_moengage(account):

    """Map account fields from CRM to MoEngage."""
    try:

        
        new_sagecompany_metadata_response = await fetch_new_sagecompany_metadata("statecode")

        print("new_sagecompany_metadata_response")
        print(new_sagecompany_metadata_response)
        new_sagecompany_options = {
            option["value"]: option["label"]
            for option in new_sagecompany_metadata_response["options"]
        }
        print("new_sagecompany_options")
        print(new_sagecompany_options)

        print("new_sagecompany")
        new_sagecompany_value = account.get("new_sagecompany", None)
        print("new_sagecompany_value")
        print(new_sagecompany_value)
        sagecompany_type = new_sagecompany_options.get(new_sagecompany_value, "")
        print("sagecompany_type")
        print(sagecompany_type)

        



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
            "account_new_resalerevenue": account.get("new_resalerevenue", "") or "",
            "account_new_lastresaledate": account.get("new_lastresaledate", "") or "",
            "account_new_resaleflagsage": account.get("new_resaleflagsage", "") or "",
            "account_new_smarevenue": account.get("new_smarevenue", "") or "",
            "account_new_sagecompany": sagecompany_type if sagecompany_type else "",
            # "account_new_sagecompany": account.get("new_sagecompany", "") or "",
            "account_new_faceliftemail": account.get("new_faceliftemail", "") or "",
            "account_new_hseqemail": account.get("new_hseqemail", "") or "",
            "account_new_excel": account.get("new_excel", "") or "",
            "account_new_resaleemail": account.get("new_resaleemail", "") or "",
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
        raise HTTPException(status_code=500,detail="failed in map-to-account function")



@router.get("/sync")
async def sync_accounts():
    """Fetch accounts from CRM and send them to MoEngage."""
    try:
        print("entered sync accounts")

        # Fetch the accounts from CRM (already filtered by created and modified dates)
        accounts_response = await fetch_accounts()

        # Extract the accounts directly from the response
        all_accounts = accounts_response.get("accounts", [])
        created_on_accounts = accounts_response.get("created_on_accounts", [])
        modified_on_accounts = accounts_response.get("modified_on_accounts", [])

        # Send the accounts to MoEngage with the necessary categorization
        await send_to_moengage(all_accounts, created_on_accounts, modified_on_accounts)

        return {"status": "Accounts synchronized successfully to MoEngage"}

    except Exception as e:
        error_message = f"Error during sync-Accounts: {str(e)}"
        log_error(S3_BUCKET_NAME, error_message)
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


async def send_to_moengage(all_accounts, created_on_accounts, modified_on_accounts):
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

    # Process All Accounts
    for account in all_accounts:
        email = account.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_all += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_all.append(record)
            print(f"Account {email} has no valid email address")
            continue

        payload = await map_account_to_moengage(account)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                print(f"Account sent successfully for {account['emailaddress1']}")
                success_all += 1
                record = {"email": account['emailaddress1'], "status": response.text}
                success_records_all.append(record)
            else:
                fail_all += 1
                record = {"email": account['emailaddress1'], "status": response.text}
                failed_records_all.append(record)
                await send_to_SQS(payload)
                print(f"Failed to send account {account['emailaddress1']}: {response.text}")
                error_message = f"Failed to send account {account['emailaddress1']}: {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
        except Exception as e:
            error_message = f"Error Occurred while sending the payload to MoEngage: {str(e)}"
            log_error(S3_BUCKET_NAME, error_message)
            print(e)
            raise HTTPException(status_code=500, detail=f"{str(e)}")

    # Process Created On Accounts
    for account in created_on_accounts:
        email = account.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_created += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_created.append(record)
            continue

        payload = await map_account_to_moengage(account)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                success_created += 1
                record = {"email": account['emailaddress1'], "status": response.text}
                success_records_created.append(record)
            else:
                fail_created += 1
                record = {"email": account['emailaddress1'], "status": response.text}
                failed_records_created.append(record)
                await send_to_SQS(payload)
                error_message = f"Failed to send account {account['emailaddress1']}: {response.text}"
                log_error(S3_BUCKET_NAME, error_message)
        except Exception as e:
            error_message = f"Error Occurred while sending the payload to MoEngage: {str(e)}"
            log_error(S3_BUCKET_NAME, error_message)
            print(e)
            raise HTTPException(status_code=500, detail=f"{str(e)}")

    # Process Modified On Accounts
    for account in modified_on_accounts:
        email = account.get('emailaddress1', '')
        if not email or email.strip() == "":
            fail_modified += 1
            record = {"email": email, "status": "Email missing or invalid"}
            failed_records_modified.append(record)
            continue

        payload =await map_account_to_moengage(account)
        try:
            response = requests.post(MOENGAGE_API_URL_Test, json=payload, headers=headers)
            if response.status_code == 200:
                success_modified += 1
                record = {"email": account['emailaddress1'], "status": response.text}
                success_records_modified.append(record)
            else:
                fail_modified += 1
                record = {"email": account['emailaddress1'], "status": response.text}
                failed_records_modified.append(record)
                await send_to_SQS(payload)
                error_message = f"Failed to send account {account['emailaddress1']}: {response.text}"
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
        "total_accounts": {
            "all": len(all_accounts),
            "created": len(created_on_accounts),
            "modified": len(modified_on_accounts)
        },
        "success_records_all": success_records_all,
        "failed_records_all": failed_records_all,
        "success_records_created": success_records_created,
        "failed_records_created": failed_records_created,
        "success_records_modified": success_records_modified,
        "failed_records_modified": failed_records_modified
    }, indent=4)

    log_processedRecords(S3_BUCKET_NAME, log_message)



# async def sync_accounts():
#     """Fetch accounts from CRM and send them to MoEngage."""
    
#     try:
#         print("entered sync accounts")
#         accounts_response = await fetch_accounts()
#         accounts = accounts_response.get("accounts", [])

#         await send_to_moengage(accounts)

        
#         return {"status": "Accounts synchronized to moengage.Please verify the status"}


#     except Exception as e:
#         error_message = f"Error during sync-Accounts: {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500, detail=f"Error: {str(e)}")




# async def send_to_moengage(accounts):
#     success_count = 0
#     fail_count = 0
#     success_records = []
#     failed_records = []

#     headers = {
#         'Authorization': token_moe,
#         'Content-Type': 'application/json',
#         'MOE-APPKEY': '6978DCU8W19J0XQOKS7NEE1C'
#     }

#     try:
#         # Send accounts to MoEngage
#         for account in accounts:
#             # Check if emailaddress1 is valid
#             email = account.get('emailaddress1', '')  # Get email and strip any surrounding spaces
#             if not email or email.strip() == "":  # If email is empty or null
#                 fail_count += 1
#                 record = {
#                     "email": email,
#                     "status": "Email missing or invalid"
#                 }
#                 failed_records.append(record)
#                 print(f"Account {email} has no valid email address")
#                 continue  # Skip this account and move to the next one
            
#             # If email is valid, proceed to send to MoEngage
#             payload = map_account_to_moengage(account)
#             try:
#                 response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)
#                 if response.status_code == 200:
#                     success_count += 1
#                     record = {
#                         "email": email,
#                         "status": response.text
#                     }
#                     success_records.append(record)
#                     print(f"Account {email} sent successfully")
#                 else:
#                     fail_count += 1
#                     record = {
#                         "email": email,
#                         "status": response.text
#                     }
#                     failed_records.append(record)
#                     await send_to_SQS(payload)
#                     print(f"Account {email} failed with error: {response.text}")
#                     error_message = f"Failed to send account {email}: {response.text}"
#                     log_error(S3_BUCKET_NAME, error_message)

#             except Exception as e:
#                 print(e)
#                 error_message = f"Error occurred while sending the payload to MoEngage: {str(e)}"
#                 log_error(S3_BUCKET_NAME, error_message)
#                 raise HTTPException(status_code=500, details=f"{str(e)}")

#         # Log processed records
#         log_message = json.dumps({
#             "timestamp": datetime.utcnow().isoformat(),
#             "success_count": success_count,
#             "fail_count": fail_count,
#             "total_accounts": len(accounts),
#             "success_records": success_records,
#             "failed_records": failed_records
#         }, indent=4)

#         log_processedRecords(S3_BUCKET_NAME, log_message)

#         return {"status": "Accounts synchronized successfully"}
    
#     except Exception as e:
#         error_message = f"Error while sending accounts: {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500, details=f"{str(e)}")


# async def send_to_moengage(accounts):

#     success_count=0
#     fail_count=0

#     success_records=[]
#     failed_records=[]

#     headers = {
#         'Authorization': token_moe,
#         'Content-Type': 'application/json',
#         'MOE-APPKEY':'6978DCU8W19J0XQOKS7NEE1C'
#     }

#     try:      
       
#         # Send accounts to MoEngage
#         for account in accounts:
#             payload = map_account_to_moengage(account)
#             try:
#                 response = requests.post(MOENGAGE_API_URL, json=payload, headers=headers)
#                 if response.status_code == 200:
#                     success_count+=1
#                     record = {
#                         "email": account['emailaddress1'],
#                         "status": response.text
#                     }
#                     success_records.append(record)
#                     print(success_records)
#                     print(f"Account {account['emailaddress1']} sent successfully")
#                 else:
#                     fail_count += 1
#                     record = {
#                         "email": account['emailaddress1'],
#                         "status": response.text
#                     }
#                     failed_records.append(record)
#                     await send_to_SQS(payload)
#                     print(failed_records)
#                     error_message = f"Failed to send account {account['emailaddress1']}: {response.text}"
#                     log_error(S3_BUCKET_NAME, error_message)  # Log the error
#                     print(f"Account {account['emailaddress1']} failed with error: {response.text}")

#             except Exception as e:
#                 print(e)
#                 error_message=f"Error Occured while sending the payload to moengage:{str(e)}"
#                 log_error(S3_BUCKET_NAME, error_message)
#                 raise HTTPException(status_code=500,details=f"{str(e)}")

#         log_message = json.dumps({
#                 "timestamp": datetime.utcnow().isoformat(),
#                 "success_count": success_count,
#                 "fail_count": fail_count,
#                 "total_accounts": len(accounts),
#                 "success_records": success_records,
#                 "failed_records": failed_records
#             }, indent=4)
        
#         log_processedRecords(S3_BUCKET_NAME, log_message)

#         return {"status": "Accounts synchronized successfully"}
#     except Exception as e:
#         error_message = f"Error while sending accounts : {str(e)}"
#         log_error(S3_BUCKET_NAME, error_message)
#         raise HTTPException(status_code=500,details=f"{str(e)}")
    





   


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
    

# async def fetch_statuscode_metadata(attribute: str = Query("new_sagecompany", description="Logical name of the attribute to fetch metadata for")):
   

#     global global_token


#     token = global_token
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Content-Type": "application/json",
#     }

#     metadata_url = f"{CRM_API_URL}/api/data/v9.0/EntityDefinitions(LogicalName='account')/Attributes(LogicalName='{attribute}')/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet"
#     print("Metadata URL:", metadata_url)
#     try:
#         # Fetch metadata
#         response = httpx.get(metadata_url, headers=headers)
#         response.raise_for_status()

#         # Extract and return relevant parts of the response
#         data = response.json()
#         print("Response Data:")
#         print(data)  # Debug: print the full response data

#         # Check if OptionSet exists in the response
#         if "OptionSet" not in data or not data["OptionSet"].get("Options"):
#             raise HTTPException(status_code=404, detail="OptionSet not found or empty in the response")

#         data = response.json()

#         attribute_display_name = data.get("DisplayName", {}).get("UserLocalizedLabel", {}).get("Label", "No label found")
#         # Extract options and labels
#         options = [
#             {
#                 "value": option.get("Value"),
#                 "label": option.get("Label", {}).get("UserLocalizedLabel", {}).get("Label", "No label found"),
#             }
#             for option in data["OptionSet"].get("Options", [])
#         ]
#         print("options")
#         print(options)


#         return {
#             "attribute": attribute,
#             "display_name": attribute_display_name,
#             "options": options,
#         }

#     except httpx.HTTPStatusError as e:
#         raise HTTPException(status_code=e.response.status_code, detail=f"HTTP Error: {e.response.text}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"An error occurred while fetching statuscode metadata: {str(e)}")


# async def fetch_statecode_metadata(attribute: str = Query("new_sagecompany", description="Logical name of the attribute to fetch metadata for")):
#     global global_token

#     token = global_token
#     print("token:", token)  # Debug: print the token
#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Content-Type": "application/json",
#     }

#     # Updated URL for fetching statecode metadata
#     metadata_url = f"{CRM_API_URL}/api/data/v9.0/EntityDefinitions(LogicalName='account')/Attributes(LogicalName='new_sagecompany')/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet"
#     print("Metadata URL:", metadata_url)  # Debug: print the URL being requested

#     try:
#         # Fetch metadata
#         response = httpx.get(metadata_url, headers=headers)
#         response.raise_for_status()

#         # Extract the JSON response
#         data = response.json()

#         print("Response Data:")
#         print(data)  # Debug: print the full response data

#         # Check if OptionSet exists and if it contains Options
#         option_set = data.get('value', [])[0].get('OptionSet', {})
#         if not option_set or 'Options' not in option_set or not option_set['Options']:
#             print("OptionSet not found or empty.")  # Debug: if OptionSet is missing or empty
#             raise HTTPException(status_code=404, detail="OptionSet not found or empty in the response")

#         # Debug: Check if 'Options' are present
#         options_data = option_set['Options']
#         print("Options Data:", options_data)  # Debug: print the options data

#         # Extract options with State and Label
#         options = [
#             {
#                 "state": option.get("State"),
#                 "label": option.get("Label", {}).get("UserLocalizedLabel", {}).get("Label", "No label found"),
#             }
#             for option in options_data
#         ]

#         print("Extracted Options:")
#         print(options)  # Debug: print the extracted options list

#         # If the attribute is found, return the options
#         return {
#             "attribute": attribute,
#             "options": options,
#         }

#     except httpx.HTTPStatusError as e:
#         print(f"HTTP Error: {e.response.status_code} - {e.response.text}")  # Debug: log the error response
#         raise HTTPException(status_code=e.response.status_code, detail=f"HTTP Error: {e.response.text}")
#     except Exception as e:
#         print(f"An error occurred: {str(e)}")  # Debug: log any other errors
#         raise HTTPException(status_code=500, detail=f"An error occurred while fetching statecode metadata: {str(e)}")


async def fetch_new_sagecompany_metadata(attribute: str = Query("new_sagecompany", description="Logical name of the attribute to fetch metadata for")):
    global global_token

    token = global_token
    print("token:", token)  # Debug: print the token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Updated URL for fetching statecode metadata
    metadata_url = f"{CRM_API_URL}/api/data/v9.0/EntityDefinitions(LogicalName='account')/Attributes(LogicalName='new_sagecompany')/Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$expand=OptionSet"
    print("Metadata URL:", metadata_url)  # Debug: print the URL being requested

    try:
        # Fetch metadata
        async with httpx.AsyncClient() as client:  # Use an asynchronous HTTP client
            response = await client.get(metadata_url, headers=headers)
            response.raise_for_status()

        # Extract the JSON response
        data = response.json()

        print("Response Data:")
        print(data)  # Debug: print the full response data

        # Check if OptionSet exists and if it contains Options
        option_set = data.get('OptionSet', {})
        if not option_set or 'Options' not in option_set or not option_set['Options']:
            print("OptionSet not found or empty.")  # Debug: if OptionSet is missing or empty
            raise HTTPException(status_code=404, detail="OptionSet not found or empty in the response")

        # Debug: Check if 'Options' are present
        options_data = option_set['Options']
        print("Options Data:", options_data)  # Debug: print the options data

        # Extract options with State and Label
        options = [
            {
                "value": option.get("Value"),
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

