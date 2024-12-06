import httpx
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# CRM_TOKEN_URL = os.getenv("CRM_TOKEN_URL")
# CRM_CLIENT_ID = os.getenv("CRM_CLIENT_ID")
# CRM_CLIENT_SECRET = os.getenv("CRM_CLIENT_SECRET")
# CRM_RESOURCE = os.getenv("CRM_RESOURCE")
CRM_TOKEN_URL="https://login.microsoftonline.com/11abf71b-55aa-4f1e-8a9d-4a801bdbee28/oauth2/token"
CRM_API_URL="https://afi-group.crm11.dynamics.com/api/data/v9.0"
MOENGAGE_API_URL = "https://api.moengage.com/v1/customers/batch"

CRM_CLIENT_ID = "111ed0aa-ce80-4ed6-a4da-d1d1bba1aeac"
CRM_CLIENT_SECRET = "lT28Q~oySx.eCcPGKGn0FVlg1TeBUdDYz1ec~bnb"
CRM_RESOURCE = "https://afi-group.crm11.dynamics.com"
# print(CRM_TOKEN_URL)
async def authenticate_crm():
    """Authenticate and get a CRM access token."""
    # Check if all required environment variables are loaded
    if not CRM_TOKEN_URL or not CRM_CLIENT_ID or not CRM_CLIENT_SECRET or not CRM_RESOURCE:
        raise ValueError("Missing one or more required environment variables.")

    # Prepare data for token request
    data = {
        "grant_type": "client_credentials",
        "client_id": CRM_CLIENT_ID,
        "client_secret": CRM_CLIENT_SECRET,
        "resource": CRM_RESOURCE,
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
