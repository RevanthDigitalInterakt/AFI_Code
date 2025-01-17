import json
import boto3
from datetime import datetime,timedelta
from fastapi import FastAPI
from sourcecode.routers import leads, Accounts, contacts
from mangum import Mangum

app = FastAPI()
handler = Mangum(app)

# Initialize the S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'crmtomoetestattributes'
DAILY_COUNT_FOLDER = 'DailyCount/'  # New folder for daily counts

@app.get("/")
async def root():
    return {"message": "Please Navigate to Swagger Docs to see end points. Hit /docs with local url"}

def get_files_in_bucket(file_type):
    
    #Lists all files in the 'processedRecords' folder in the S3 bucket for a given type.
   
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="processedRecords/")
    if 'Contents' not in response:
        print("No files found in the 'processedRecords' folder.")
        return []

    # Filter files by type
    files = [file['Key'] for file in response['Contents'] if file_type in file['Key']]
    return files




async def get_records_for_day(file_type: str, date_str: str):
    # Initialize counts for all fields
    success_all = 0
    fail_all = 0
    success_created = 0
    fail_created = 0
    success_modified = 0
    fail_modified = 0
    total_all = 0
    total_created = 0
    total_modified = 0
    moengage_created = 0  # Count for unique emails in "success_records_created"
    moengage_modified = 0  # Count for unique emails in "success_records_modified"

    all_files = get_files_in_bucket(file_type)
    filtered_files = [file for file in all_files if date_str in file]

    if not filtered_files:
        print(f"No files found for type {file_type} on date {date_str}")
        return {
            "success_all": success_all,
            "fail_all": fail_all,
            "success_created": success_created,
            "fail_created": fail_created,
            "success_modified": success_modified,
            "fail_modified": fail_modified,
            "moengage_created": moengage_created,  # Include the count in the return value
            "moengage_modified": moengage_modified,  # Include the count in the return value
            "total": {
                "all": total_all,
                "created": total_created,
                "modified": total_modified
            }
        }

    for file_key in filtered_files:
        if not file_key.endswith(".json"):
            print(f"File {file_key} is not a JSON file. Skipping.")
            continue

        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        body = obj['Body'].read()

        if not body.strip():  # Skip empty files
            print(f"File {file_key} is empty. Skipping.")
            continue

        try:
            data = json.loads(body.decode('utf-8'))
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in file {file_key}: {e}. Skipping.")
            continue

        # Extract success and fail counts for different categories
        success_all += data.get('success_all', 0)
        fail_all += data.get('fail_all', 0)
        success_created += data.get('success_created', 0)
        fail_created += data.get('fail_created', 0)
        success_modified += data.get('success_modified', 0)
        fail_modified += data.get('fail_modified', 0)

        # Extract emails for the "success_records_created" and "success_records_modified" fields
        # Ensure no duplicates by using set
        success_created_emails = {record["email"] for record in data.get("success_records_created", []) if record.get("email")}
        success_modified_emails = {record["email"] for record in data.get("success_records_modified", []) if record.get("email")}

        # Count unique emails
        moengage_created += len(success_created_emails)
        moengage_modified += len(success_modified_emails)

        # Determine the appropriate field based on the file type (leads, accounts, contacts)
        if file_type == 'leads':
            if 'total_leads' in data:
                total_all += data['total_leads'].get('all', 0)
                total_created += data['total_leads'].get('created', 0)
                total_modified += data['total_leads'].get('modified', 0)
        elif file_type == 'accounts':
            if 'total_accounts' in data:
                total_all += data['total_accounts'].get('all', 0)
                total_created += data['total_accounts'].get('created', 0)
                total_modified += data['total_accounts'].get('modified', 0)
        elif file_type == 'contacts':
            if 'total_contacts' in data:
                total_all += data['total_contacts'].get('all', 0)
                total_created += data['total_contacts'].get('created', 0)
                total_modified += data['total_contacts'].get('modified', 0)

    return {
        "success_all": success_all,
        "fail_all": fail_all,
        "success_created": success_created,
        "fail_created": fail_created,
        "success_modified": success_modified,
        "fail_modified": fail_modified,
        "moengage_created": moengage_created,  # Include the count in the return value
        "moengage_modified": moengage_modified,  # Include the count in the return value
        "total": {
            "all": total_all,
            "created": total_created,
            "modified": total_modified
        }
    }



def save_daily_count(date_str, counts):
    """
    Saves the daily counts as a JSON file in the 'DailyCount' folder in S3.
    
    Args:
    - date_str (str): The date string in 'YYYY-MM-DD' format.
    - counts (dict): The daily counts data (total records, success, fail counts).
    """
    file_key = f"{DAILY_COUNT_FOLDER}{date_str}_count.json"
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=file_key,
        Body=json.dumps(counts),
        ContentType='application/json'
    )
    print(f"Daily count saved to {file_key}")


# Example usage: Get the total records for leads, accounts, and contacts today
@app.get("/Total_Records_Today")
async def total_records_today():
    # date_today = datetime.utcnow().strftime('%Y-%m-%d')
    date_today=(datetime.utcnow() - timedelta(days=0)).strftime('%Y-%m-%d')
    # Process leads
    lead_data = await get_records_for_day('leads', date_today)

    # Process accounts
    account_data = await get_records_for_day('accounts', date_today)

    # Process contacts
    contact_data = await get_records_for_day('contacts', date_today)

    # Compile the counts into a dictionary-
    total_counts = {
        "leads": lead_data,
        "accounts": account_data,
        "contacts": contact_data
    }

    # Save the daily counts to the S3 folder
    save_daily_count(date_today, total_counts)

    return total_counts












# Include the routers
app.include_router(leads.router)
app.include_router(Accounts.router, prefix="/accounts", tags=["accounts"])
app.include_router(contacts.router, prefix="/contacts", tags=["contacts"])




