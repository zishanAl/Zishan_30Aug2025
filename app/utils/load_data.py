import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime
from app.db import SessionLocal, engine
from app.models import StoreStatus, BusinessHours, StoreTimezone, Base

# Create tables if not exist
Base.metadata.create_all(bind=engine)

def load_store_status(file_path: str, db: Session):
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        # Handle timestamp ending with ' UTC'
        ts = str(row["timestamp_utc"]).replace(" UTC", "")
        record = StoreStatus(
            store_id=str(row["store_id"]),
            timestamp_utc=datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f"),
            status=row["status"]
        )
        db.add(record)
    db.commit()
    print("✅ Store status data loaded.")

def load_business_hours(file_path: str, db: Session):
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        record = BusinessHours(
            store_id=str(row["store_id"]),
            day_of_week=int(row["dayOfWeek"]),
            start_time_local=datetime.strptime(row["start_time_local"], "%H:%M:%S").time(),
            end_time_local=datetime.strptime(row["end_time_local"], "%H:%M:%S").time(),
        )
        db.add(record)
    db.commit()
    print("✅ Business hours data loaded.")

def load_store_timezone(file_path: str, db: Session):
    df = pd.read_csv(file_path)

    for _, row in df.iterrows():
        record = StoreTimezone(
            store_id=str(row["store_id"]),
            timezone_str=row["timezone_str"]
        )
        db.add(record)
    db.commit()
    print("✅ Store timezone data loaded.")

def run_ingestion():
    db = SessionLocal()
    try:
        load_store_status("data/store_status.csv", db)
        load_business_hours("data/menu_hours.csv", db)
        load_store_timezone("data/timezones.csv", db)
    finally:
        db.close()

if __name__ == "__main__":
    run_ingestion()
