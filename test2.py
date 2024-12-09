import json
import boto3
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'apierrorlog'
PROCESSED_LOGS_FOLDER = 'processedRecords/'

def get_files_in_bucket(file_type):
    """
    Lists all files in the S3 bucket for a given type (leads, accounts, contacts).
    
    Args:
    - file_type (str): Type of file to filter ('leads', 'accounts', 'contacts')
    
    Returns:
    - files (list): List of file keys matching the file type.
    """
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
    if 'Contents' not in response:
        print("No files found in the bucket.")
        return []

    # Filter out files that match the file_type
    files = [file['Key'] for file in response['Contents'] if file_type in file['Key']]
    # print(f"{file_type.capitalize()} files found: {files}")
    return files

def get_records_for_day(file_type, date_str):
    """
    Aggregates the total records, success, and failure counts for the given file type logs of a given day.
    
    Args:
    - file_type (str): Type of file to filter ('leads', 'accounts', 'contacts')
    - date_str (str): The date string in 'YYYY-MM-DD' format.
    
    Returns:
    - total_records (int): The total records processed for the day from the specified file type.
    - success_count (int): The total successful records for the day from the specified file type.
    - fail_count (int): The total failed records for the day from the specified file type.
    """
    total_records = 0
    success_count = 0
    fail_count = 0

    # List all files of the given file type
    all_files = get_files_in_bucket(file_type)
    filtered_files = [file for file in all_files if date_str in file]

    # print(f"{file_type.capitalize()} files found for {date_str}: {filtered_files}")

    if not filtered_files:
        # print(f"No {file_type} files found for date {date_str}")
        return total_records, success_count, fail_count

    # Iterate through the filtered files
    for file_key in filtered_files:
        print(f"Processing {file_type} file: {file_key}")

        # Fetch the file from S3
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))

        # print(f"File content: {data}")

        # Process success and failed records from the file
        processed_count = data.get('success_count', 0)
        total_records += processed_count
        success_count += processed_count

        failed_records = data.get('failed_records', [])
        fail_count += len(failed_records)

    return total_records, success_count, fail_count

# Example usage: Get the total records for leads, accounts, and contacts today
date_today = datetime.utcnow().strftime('%Y-%m-%d')

# Process leads
total_records_leads, total_success_leads, total_failed_leads = get_records_for_day('leads', date_today)
print(f"Total leads records processed today ({date_today}): {total_records_leads}")
print(f"Total successful leads records today: {total_success_leads}")
print(f"Total failed leads records today: {total_failed_leads}")

# Process accounts
total_records_accounts, total_success_accounts, total_failed_accounts = get_records_for_day('Accounts', date_today)
print(f"Total accounts records processed today ({date_today}): {total_records_accounts}")
print(f"Total successful accounts records today: {total_success_accounts}")
print(f"Total failed accounts records today: {total_failed_accounts}")

# Process contacts
total_records_contacts, total_success_contacts, total_failed_contacts = get_records_for_day('contacts', date_today)
print(f"Total contacts records processed today ({date_today}): {total_records_contacts}")
print(f"Total successful contacts records today: {total_success_contacts}")
print(f"Total failed contacts records today: {total_failed_contacts}")
