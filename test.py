import asyncio
from sourcecode.crmAuthentication import authenticate_crm
from sourcecode.routers.leads import fetch_leads
async def get_crm_token():
    try:
        token = await authenticate_crm() # Await the asynchronous function
        print(f"Received CRM Access Token: {token}")
    except Exception as e:
        print(f"Error during CRM authentication: {e}")

if __name__ == "__main__":
    asyncio.run(get_crm_token())  # Run the async function within an event loop


