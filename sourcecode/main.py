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
S3_BUCKET_NAME = 'crmtomoe'
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
    total_records = 0
    success_count = 0
    fail_count = 0

    all_files = get_files_in_bucket(file_type)
    filtered_files = [file for file in all_files if date_str in file]

    if not filtered_files:
        print(f"No files found for type {file_type} on date {date_str}")
        return {"total_records": total_records, "success_count": success_count, "fail_count": fail_count}

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

        processed_count = data.get('success_count', 0)
        failed_records = data.get('failed_records', [])

        total_records += processed_count
        success_count += processed_count
        fail_count += len(failed_records)

    return {"total_records": total_records, "success_count": success_count, "fail_count": fail_count}

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
    date_today=(datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
    # Process leads
    lead_data = await get_records_for_day('leads', date_today)

    # Process accounts
    account_data = await get_records_for_day('accounts', date_today)

    # Process contacts
    contact_data = await get_records_for_day('contacts', date_today)

    # Compile the counts into a dictionary
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




