import pandas as pd
from datetime import timedelta, datetime, time, date
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db import SessionLocal
from app.models import StoreStatus, BusinessHours, StoreTimezone
import pytz
from typing import List, Tuple, Union

# ==================================================
# Helpers
# ==================================================

def get_max_timestamp(db: Session) -> datetime:
    """Fetch max timestamp from store_status table (acts as 'now', in UTC)."""
    # Stored as naive UTC in DB (per our ingestion), so we treat it as UTC later.
    return db.query(func.max(StoreStatus.timestamp_utc)).scalar()

def get_store_timezone(db: Session, store_id: str):
    """Get timezone object for store (default America/Chicago)."""
    tz_row = db.query(StoreTimezone).filter(StoreTimezone.store_id == store_id).first()
    return pytz.timezone(tz_row.timezone_str if tz_row else "America/Chicago")

def _normalize_to_time(v: Union[time, timedelta, str]) -> time:
    """
    Ensure we have a datetime.time object.
    Accepts datetime.time, timedelta (seconds from midnight), or "HH:MM:SS" string.
    """
    if isinstance(v, time):
        return v
    if isinstance(v, timedelta):
        total_seconds = int(v.total_seconds()) % (24 * 3600)
        hh = total_seconds // 3600
        mm = (total_seconds % 3600) // 60
        ss = total_seconds % 60
        return time(hh, mm, ss)
    if isinstance(v, str):
        return datetime.strptime(v, "%H:%M:%S").time()
    raise TypeError(f"Unsupported type for business hour field: {type(v)}")

def get_business_hours(db: Session, store_id: str, day_of_week: int) -> List[Tuple[time, time]]:
    """
    Business hours for a store on a given weekday (0=Mon..6=Sun).
    If no rows → open 24x7 for that day: 00:00:00 → 23:59:59
    """
    rows = db.query(BusinessHours).filter(
        BusinessHours.store_id == store_id,
        BusinessHours.day_of_week == day_of_week
    ).all()

    if not rows:
        return [(time(0, 0, 0), time(23, 59, 59))]

    return [(_normalize_to_time(r.start_time_local), _normalize_to_time(r.end_time_local)) for r in rows]

def _to_local(tz, dt_utc: datetime) -> datetime:
    """Convert a naive UTC datetime to timezone-aware local time."""
    if dt_utc.tzinfo is None:
        dt_utc = pytz.UTC.localize(dt_utc)
    else:
        dt_utc = dt_utc.astimezone(pytz.UTC)
    return dt_utc.astimezone(tz)

def _localize_naive(tz, d: date, t: time) -> datetime:
    """Create tz-aware local datetime for given local date+time using pytz.localize (DST-safe)."""
    return tz.localize(datetime.combine(d, t))

# ==================================================
# Core Report Logic
# ==================================================

def calculate_uptime_downtime(store_id: str, db: Session, now: datetime):
    """
    Compute uptime/downtime for last hour (minutes), last day (hours), last week (hours),
    counting ONLY within business hours. Interpolates status between polls.
    """
    tz = get_store_timezone(db, store_id)

    # Treat `now` (naive UTC) as aware UTC, then also compute local-now.
    now_utc = pytz.UTC.localize(now) if now.tzinfo is None else now.astimezone(pytz.UTC)
    now_local = now_utc.astimezone(tz)

    # Define window starts in UTC (aware), then we’ll compare in local
    hour_start_utc = now_utc - timedelta(hours=1)
    day_start_utc = now_utc - timedelta(days=1)
    week_start_utc = now_utc - timedelta(weeks=1)

    # Fetch logs within last week (to cover all three windows)
    logs = db.query(StoreStatus).filter(
        StoreStatus.store_id == store_id,
        StoreStatus.timestamp_utc >= week_start_utc.replace(tzinfo=None)  # DB holds naive UTC
    ).order_by(StoreStatus.timestamp_utc.asc()).all()

    if not logs:
        return {
            "store_id": store_id,
            "uptime_last_hour": 0, "downtime_last_hour": 0,
            "uptime_last_day": 0, "downtime_last_day": 0,
            "uptime_last_week": 0, "downtime_last_week": 0,
        }

    # Logs → DataFrame, then build local timestamps
    df = pd.DataFrame([{"timestamp_utc": l.timestamp_utc, "status": l.status} for l in logs])
    # Make UTC aware then convert to local
    df["timestamp_local"] = df["timestamp_utc"].dt.tz_localize("UTC").dt.tz_convert(tz)

    # Interpolate: each status persists until next poll; last one persists to now
    df["next_time"] = df["timestamp_local"].shift(-1)
    df["next_time"] = df["next_time"].fillna(now_local)

    results = {
        "store_id": store_id,
        "uptime_last_hour": 0.0, "downtime_last_hour": 0.0,   # minutes
        "uptime_last_day": 0.0,  "downtime_last_day": 0.0,    # hours
        "uptime_last_week": 0.0, "downtime_last_week": 0.0,   # hours
    }

    # Iterate each observed interval
    for _, row in df.iterrows():
        start_local: datetime = row["timestamp_local"]
        end_local: datetime = row["next_time"]
        is_active: bool = (row["status"] == "active")

        # Walk across each calendar day touched by this interval
        day_cursor = start_local
        while day_cursor.date() <= end_local.date():
            dow = day_cursor.weekday()
            business_ranges = get_business_hours(db, store_id, dow)

            for bh_start_t, bh_end_t in business_ranges:
                # Create tz-aware local datetimes for business hours on this specific day
                bh_start_local = _localize_naive(tz, day_cursor.date(), bh_start_t)
                bh_end_local = _localize_naive(tz, day_cursor.date(), bh_end_t)

                # (Optional) handle cross-midnight hours; split if needed
                segments = []
                if bh_end_local <= bh_start_local:
                    # e.g., 22:00 -> 02:00 next day; split into two day-bound segments
                    end_of_day = _localize_naive(tz, day_cursor.date(), time(23, 59, 59))
                    segments.append((bh_start_local, end_of_day))
                    next_day = (day_cursor + timedelta(days=1)).date()
                    start_of_next = _localize_naive(tz, next_day, time(0, 0, 0))
                    next_end = _localize_naive(tz, next_day, bh_end_t)
                    segments.append((start_of_next, next_end))
                else:
                    segments.append((bh_start_local, bh_end_local))

                for seg_start, seg_end in segments:
                    # intersect the observation interval with the business-hours segment and "now"
                    overlap_start = max(start_local, seg_start)
                    overlap_end = min(end_local, seg_end, now_local)

                    if overlap_start < overlap_end:
                        # We now further clip to hour/day/week windows (in local time)
                        for win_name, win_start_utc, up_key, down_key, unit in [
                            ("hour", hour_start_utc, "uptime_last_hour", "downtime_last_hour", 1),   # minutes
                            ("day",  day_start_utc,  "uptime_last_day",  "downtime_last_day",  60),  # hours
                            ("week", week_start_utc, "uptime_last_week", "downtime_last_week", 60),  # hours
                        ]:
                            win_start_local = win_start_utc.astimezone(tz)
                            win_end_local = now_local  # window ends at now

                            w_start = max(overlap_start, win_start_local)
                            w_end = min(overlap_end, win_end_local)

                            if w_start < w_end:
                                minutes = (w_end - w_start).total_seconds() / 60.0
                                if is_active:
                                    results[up_key] += minutes / unit
                                else:
                                    results[down_key] += minutes / unit

            day_cursor = day_cursor + timedelta(days=1)

    # Round small float noise
    for k in ["uptime_last_hour", "downtime_last_hour",
              "uptime_last_day", "downtime_last_day",
              "uptime_last_week", "downtime_last_week"]:
        results[k] = float(round(results[k], 6))

    return results

# ==================================================
# Report Generator
# ==================================================

def generate_report(output_path="output/report.csv"):
    db = SessionLocal()
    try:
        now = get_max_timestamp(db)  # hard-coded 'current' time (UTC, naive)
        store_ids = [row[0] for row in db.query(StoreStatus.store_id).distinct()]

        report_rows = [calculate_uptime_downtime(store_id, db, now) for store_id in store_ids]

        df = pd.DataFrame(report_rows)
        df.to_csv(output_path, index=False)
        print(f"Report generated: {output_path}")
        return output_path
    finally:
        db.close()

if __name__ == "__main__":
    generate_report()
