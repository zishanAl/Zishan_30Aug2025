from fastapi import FastAPI
from app.db import Base, engine
from app.api import report_api

app = FastAPI()

# Create tables
Base.metadata.create_all(bind=engine)

# Include APIs
app.include_router(report_api.router)

@app.get("/")
def read_root():
    return {"message": "Store Monitoring API is running 🚀"}
