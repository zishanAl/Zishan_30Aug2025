import pandas as pd
from datetime import timedelta, datetime, time, date
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.db import SessionLocal
from app.models import StoreStatus, BusinessHours, StoreTimezone
import pytz
from typing import List, Tuple, Union, Dict

def get_max_timestamp(db: Session) -> datetime:
    return db.query(func.max(StoreStatus.timestamp_utc)).scalar()

def get_all_store_timezones(db: Session) -> Dict[str, str]:
    rows = db.query(StoreTimezone).all()
    return {row.store_id: row.timezone_str for row in rows}

def _normalize_to_time(v: Union[time, timedelta, str]) -> time:
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

def get_all_business_hours(db: Session) -> Dict[Tuple[str, int], List[Tuple[time, time]]]:
    rows = db.query(BusinessHours).all()
    hours = {}
    for row in rows:
        key = (row.store_id, row.day_of_week)
        if key not in hours:
            hours[key] = []
        hours[key].append((_normalize_to_time(row.start_time_local), _normalize_to_time(row.end_time_local)))
    return hours

def _localize_naive(tz, d: date, t: time) -> datetime:
    return tz.localize(datetime.combine(d, t))

def calculate_uptime_downtime(store_id: str, logs_df: pd.DataFrame, now: datetime, tz_str: str,
                               all_bh: Dict[Tuple[str, int], List[Tuple[time, time]]]) -> Dict:
    tz = pytz.timezone(tz_str or "America/Chicago")
    now_utc = pytz.UTC.localize(now) if now.tzinfo is None else now.astimezone(pytz.UTC)
    now_local = now_utc.astimezone(tz)
    hour_start_utc = now_utc - timedelta(hours=1)
    day_start_utc = now_utc - timedelta(days=1)
    week_start_utc = now_utc - timedelta(weeks=1)

    if logs_df.empty:
        return {
            "store_id": store_id,
            "uptime_last_hour": 0, "downtime_last_hour": 0,
            "uptime_last_day": 0, "downtime_last_day": 0,
            "uptime_last_week": 0, "downtime_last_week": 0,
        }

    df = logs_df.copy()
    df["timestamp_local"] = df["timestamp_utc"].dt.tz_localize("UTC").dt.tz_convert(tz)
    df["next_time"] = df["timestamp_local"].shift(-1).fillna(now_local)

    results = {
        "store_id": store_id,
        "uptime_last_hour": 0.0, "downtime_last_hour": 0.0,
        "uptime_last_day": 0.0, "downtime_last_day": 0.0,
        "uptime_last_week": 0.0, "downtime_last_week": 0.0,
    }

    for _, row in df.iterrows():
        start_local, end_local = row["timestamp_local"], row["next_time"]
        is_active = (row["status"] == "active")
        day_cursor = start_local

        while day_cursor.date() <= end_local.date():
            dow = day_cursor.weekday()
            bh_ranges = all_bh.get((store_id, dow), [(time(0, 0, 0), time(23, 59, 59))])
            for bh_start, bh_end in bh_ranges:
                segments = []
                bh_start_local = _localize_naive(tz, day_cursor.date(), bh_start)
                bh_end_local = _localize_naive(tz, day_cursor.date(), bh_end)

                if bh_end_local <= bh_start_local:
                    end_of_day = _localize_naive(tz, day_cursor.date(), time(23, 59, 59))
                    segments.append((bh_start_local, end_of_day))
                    next_day = (day_cursor + timedelta(days=1)).date()
                    segments.append((_localize_naive(tz, next_day, time(0, 0, 0)), _localize_naive(tz, next_day, bh_end)))
                else:
                    segments.append((bh_start_local, bh_end_local))

                for seg_start, seg_end in segments:
                    overlap_start = max(start_local, seg_start)
                    overlap_end = min(end_local, seg_end, now_local)
                    if overlap_start < overlap_end:
                        for _, win_start_utc, up_key, down_key, unit in [
                            ("hour", hour_start_utc, "uptime_last_hour", "downtime_last_hour", 1),
                            ("day", day_start_utc, "uptime_last_day", "downtime_last_day", 60),
                            ("week", week_start_utc, "uptime_last_week", "downtime_last_week", 60),
                        ]:
                            win_start_local = win_start_utc.astimezone(tz)
                            w_start = max(overlap_start, win_start_local)
                            w_end = min(overlap_end, now_local)
                            if w_start < w_end:
                                minutes = (w_end - w_start).total_seconds() / 60.0
                                results[up_key] += minutes / unit if is_active else 0
                                results[down_key] += minutes / unit if not is_active else 0

            day_cursor += timedelta(days=1)

    for k in results:
        if k != "store_id":
            results[k] = round(results[k], 6)

    return results

def generate_report(output_path="output/report.csv"):
    db = SessionLocal()
    try:
        now = get_max_timestamp(db)

        # Optimization 1: load all logs at once (reduce per-store DB call)
        week_start = now - timedelta(weeks=1)
        logs = db.query(StoreStatus).filter(
            StoreStatus.timestamp_utc >= week_start.replace(tzinfo=None)
        ).all()
        logs_df = pd.DataFrame([{
            "store_id": l.store_id,
            "timestamp_utc": l.timestamp_utc,
            "status": l.status
        } for l in logs])

        # Optimization 2: preload all timezones and business hours
        tz_map = get_all_store_timezones(db)
        all_bh = get_all_business_hours(db)

        report_rows = []
        for store_id in logs_df["store_id"].unique():
            store_logs = logs_df[logs_df["store_id"] == store_id]
            row = calculate_uptime_downtime(store_id, store_logs, now, tz_map.get(store_id), all_bh)
            report_rows.append(row)

        df = pd.DataFrame(report_rows)
        df.to_csv(output_path, index=False)
        print(f"Report generated: {output_path}")
        return output_path
    finally:
        db.close()
