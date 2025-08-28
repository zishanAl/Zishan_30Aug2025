import pandas as pd
from datetime import timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db import SessionLocal
from app.models import StoreStatus, BusinessHours, StoreTimezone
import pytz

# ==================================================
# Helpers
# ==================================================

def get_max_timestamp(db: Session):
    """Fetch max timestamp from store_status table (acts as 'now')."""
    max_ts = db.query(func.max(StoreStatus.timestamp_utc)).scalar()
    return max_ts

def get_store_timezone(db: Session, store_id: str):
    """Get timezone string for store (default America/Chicago)."""
    tz = db.query(StoreTimezone).filter(StoreTimezone.store_id == store_id).first()
    return pytz.timezone(tz.timezone_str if tz else "America/Chicago")

def get_business_hours(db: Session, store_id: str, day_of_week: int):
    """Get business hours for a store (default 24x7)."""
    rows = db.query(BusinessHours).filter(
        BusinessHours.store_id == store_id,
        BusinessHours.day_of_week == day_of_week
    ).all()

    if not rows:  # Default = open 24 hours
        return [(timedelta(hours=0), timedelta(hours=24))]

    return [(r.start_time_local, r.end_time_local) for r in rows]

# ==================================================
# Core Report Logic
# ==================================================

def calculate_uptime_downtime(store_id: str, db: Session, now):
    """
    Compute uptime/downtime for last hour, last day, last week.
    Returns dict for single store.
    """
    tz = get_store_timezone(db, store_id)

    # Define time windows
    last_hour_start = now - timedelta(hours=1)
    last_day_start = now - timedelta(days=1)
    last_week_start = now - timedelta(weeks=1)

    # Fetch logs within last week (to cover all windows)
    logs = db.query(StoreStatus).filter(
        StoreStatus.store_id == store_id,
        StoreStatus.timestamp_utc >= last_week_start
    ).order_by(StoreStatus.timestamp_utc.asc()).all()

    if not logs:
        return {
            "store_id": store_id,
            "uptime_last_hour": 0, "downtime_last_hour": 0,
            "uptime_last_day": 0, "downtime_last_day": 0,
            "uptime_last_week": 0, "downtime_last_week": 0,
        }

    # Convert logs to DataFrame for easier processing
    df = pd.DataFrame([{
        "timestamp_utc": log.timestamp_utc,
        "status": log.status
    } for log in logs])

    df["timestamp_local"] = df["timestamp_utc"].dt.tz_localize("UTC").dt.tz_convert(tz)

    # Interpolation logic
    # Assume state continues until next observation
    df["next_time"] = df["timestamp_local"].shift(-1)
    df["next_time"] = df["next_time"].fillna(pd.Timestamp(now, tz=tz))
    


    results = {
        "store_id": store_id,
        "uptime_last_hour": 0, "downtime_last_hour": 0,
        "uptime_last_day": 0, "downtime_last_day": 0,
        "uptime_last_week": 0, "downtime_last_week": 0,
    }

    # Iterate intervals
    for _, row in df.iterrows():
        start, end = row["timestamp_local"], row["next_time"]
        duration = (end - start).total_seconds() / 60  # in minutes

        # Assign to correct status
        is_active = (row["status"] == "active")

        # Add contribution to each time window
        for window, start_time, uptime_key, downtime_key, unit in [
            ("hour", last_hour_start, "uptime_last_hour", "downtime_last_hour", 1),
            ("day", last_day_start, "uptime_last_day", "downtime_last_day", 60),
            ("week", last_week_start, "uptime_last_week", "downtime_last_week", 60)
        ]:
            overlap_start = max(start, start_time.astimezone(tz))
            overlap_end = min(end, now.astimezone(tz))
            if overlap_start < overlap_end:
                overlap_minutes = (overlap_end - overlap_start).total_seconds() / 60
                if is_active:
                    results[uptime_key] += overlap_minutes / unit
                else:
                    results[downtime_key] += overlap_minutes / unit

    return results

# ==================================================
# Report Generator
# ==================================================

def generate_report(output_path="output/report.csv"):
    db = SessionLocal()
    try:
        now = get_max_timestamp(db)  # hard-coded 'current' time
        store_ids = [row[0] for row in db.query(StoreStatus.store_id).distinct()]

        report = []
        for store_id in store_ids:
            report.append(calculate_uptime_downtime(store_id, db, now))

        # Save CSV
        df = pd.DataFrame(report)
        df.to_csv(output_path, index=False)
        print(f"✅ Report generated: {output_path}")
        return output_path
    finally:
        db.close()

if __name__ == "__main__":
    generate_report()
