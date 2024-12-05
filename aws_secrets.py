
# import boto3
# from botocore.exceptions import ClientError

# secret_name = "afi/crm/test"
# region_name = "eu-north-1"

#     # Create a Secrets Manager client
# session = boto3.session.Session()
# client = session.client(
#     service_name='secretsmanager',
#     region_name=region_name
# )

# def get_secret(event,context):

    
#     try:
#         get_secret_value_response = client.get_secret_value(
#             SecretId=secret_name
#         )
#         print(get_secret_value_response)
#         secret = get_secret_value_response['SecretString']
#         print(secret)
#     except ClientError as e:
#         # For a list of exceptions thrown, see
#         # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
#         raise e

   

#     # Your code goes here.

import boto3
from botocore.exceptions import ClientError

# Set the secret name and region
secret_name = "afi/crm/test"
region_name = "eu-north-1"

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)

def get_secret(event, context):
    try:
        # Retrieve the secret value
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        print("Secret retrieved successfully:", get_secret_value_response)
        
        # Access the secret value
        secret = get_secret_value_response['SecretString']
        print("Secret string:", secret)

        return secret  # Return the secret to the calling function or log it

    except ClientError as e:
        print(f"An error occurred: {e}")
        raise e

# Example for local testing
# If you're testing locally, this will call the function and pass empty event/context.
# In AWS Lambda, the event and context are passed automatically.
if __name__ == "__main__":
    event = {}  # Empty event for testing locally
    context = {}  # Empty context for testing locally
    get_secret(event, context)
