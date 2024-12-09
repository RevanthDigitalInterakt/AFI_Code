import json
import boto3
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')
S3_BUCKET_NAME = 'apierrorlog'
PROCESSED_LOGS_FOLDER = 'processedRecords/'

def get_all_files_in_bucket():
    """
    Lists all files in the S3 bucket to help debug folder structure.
    Filters out files that do not contain 'leads' in the file name.
    """
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
    if 'Contents' not in response:
        print("No files found in the bucket.")
        return []

    # Filter out files that contain 'leads' in the file name
    files = [file['Key'] for file in response['Contents'] if 'leads' in file['Key']]
    print(f"Leads files found: {files}")
    return files

def get_leads_records_for_day(date_str):
    """
    Aggregates the total records, success, and failure counts for leads logs of a given day.
    
    Args:
    - date_str (str): The date string in 'YYYY-MM-DD' format.
    
    Returns:
    - total_records (int): The total records processed for the day from the leads files.
    - success_count (int): The total successful records for the day from the leads files.
    - fail_count (int): The total failed records for the day from the leads files.
    """
    total_records = 0
    success_count = 0
    fail_count = 0

    # List all leads files in the bucket
    all_files = get_all_files_in_bucket()
    filtered_files = [file for file in all_files if date_str in file]

    print(f"Leads files found for {date_str}: {filtered_files}")

    if not filtered_files:
        print(f"No leads files found for date {date_str}")
        return total_records, success_count, fail_count

    # Iterate through the filtered files (leads files)
    for file_key in filtered_files:
        print(f"Processing leads file: {file_key}")

        # Fetch the file from S3
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))

        print(f"File content: {data}")

        # Process success and failed records from the leads file
        processed_count = data.get('success_count', 0)
        total_records += processed_count
        success_count += processed_count

        failed_records = data.get('failed_records', [])
        fail_count += len(failed_records)

    return total_records, success_count, fail_count

# Example usage: Get the total records for leads today
date_today = datetime.utcnow().strftime('%Y-%m-%d')
total_records_today, total_success, total_failed = get_leads_records_for_day(date_today)

print(f"Total leads records processed today ({date_today}): {total_records_today}")
print(f"Total successful leads records today: {total_success}")
print(f"Total failed leads records today: {total_failed}")





















#
#
#  import json
# import boto3
# from datetime import datetime

# # Initialize the S3 client
# s3 = boto3.client('s3')
# S3_BUCKET_NAME = 'apierrorlog'
# PROCESSED_LOGS_FOLDER = 'processedRecords/'

# def get_all_files_in_bucket():
#     """
#     Lists all files in the S3 bucket to help debug folder structure.
#     """
#     response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
#     if 'Contents' not in response:
#         print("No files found in the bucket.")
#         return []

#     files = [file['Key'] for file in response['Contents']]
#     print(f"All files in the bucket: {files}")
#     return files

# def get_records_for_day(date_str):
#     """
#     Aggregates the records processed from all logs for a given day.

#     Args:
#     - date_str (str): The date string in 'YYYY-MM-DD' format.

#     Returns:
#     - total_records (int): The total records processed for the day.
#     """
#     total_records = 0

#     # List all files in the bucket to check the structure
#     all_files = get_all_files_in_bucket()
#     filtered_files = [file for file in all_files if date_str in file]

#     print(f"Files found for {date_str}: {filtered_files}")

#     if not filtered_files:
#         print(f"No files found for date {date_str}")
#         return 0

#     # Iterate through the filtered files
#     for file_key in filtered_files:
#         print(f"Processing file: {file_key}")

#         # Fetch the file from S3
#         obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
#         data = json.loads(obj['Body'].read().decode('utf-8'))

#         print(f"File content: {data}")

#         processed_count = data.get('success_count', 0)
#         print(f"Processed count (success_count): {processed_count}")

#         if processed_count == 0:
#             print(f"No 'success_count' in {file_key}. Checking alternative keys...")

#             for key in ['leads', 'accounts', 'contacts']:
#                 if key in data:
#                     processed_count = data[key]
#                     print(f"Found {key}: {processed_count}")
#                     break

#         total_records += processed_count

#     return total_records

# # Example usage: Get the total records for today
# date_today = datetime.utcnow().strftime('%Y-%m-%d')
# total_records_today = get_records_for_day(date_today)
# print(f"Total records processed today ({date_today}): {total_records_today}")
