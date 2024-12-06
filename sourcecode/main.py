from fastapi import FastAPI
from sourcecode.routers import leads ,Accounts,contacts
from mangum import Mangum

app = FastAPI()

# Include the routers
app.include_router(leads.router)
app.include_router(Accounts.router, prefix="/accounts", tags=["accounts"])
app.include_router(contacts.router, prefix="/contacts", tags=["contacts"])
# app.include_router(contacts.router)
# app.include_router(accounts.router)

handler = Mangum(app)
