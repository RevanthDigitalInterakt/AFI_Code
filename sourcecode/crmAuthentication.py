import httpx
import os
import boto3,json
from fastapi import HTTPException


# Initialize the AWS Secrets Manager client
secrets_client = boto3.client("secretsmanager")

def get_secret(secret_name: str):
    """Fetch secrets from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        elif "SecretBinary" in response:
            return json.loads(response["SecretBinary"])
    except Exception as e:
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









async def authenticate_crm():
    """Authenticate and get a CRM access token."""
    # Check if all required environment variables are loaded
    if not CRM_TOKEN_URL or not CRM_CLIENT_ID or not CRM_CLIENT_SECRET or not CRM_API_URL:
        raise ValueError("Missing one or more required environment variables.")

    # Prepare data for token request
    data = {
        "grant_type": "client_credentials",
        "client_id": CRM_CLIENT_ID,
        "client_secret": CRM_CLIENT_SECRET,
        "resource": CRM_API_URL,
    }

    # Make the POST request to authenticate asynchronously
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(CRM_TOKEN_URL, data=data)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
            # If successful, print and return the access token
            print("CRM authentication successful.")
            return response.json().get("access_token")
        except httpx.HTTPStatusError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response content: {response.text}")
            raise Exception(f"CRM Authentication failed with status code {response.status_code}")
        except Exception as err:
            print(f"Other error occurred: {err}")
            raise Exception("CRM Authentication failed due to an unexpected error.")
