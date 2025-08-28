from sqlalchemy import Column, Integer, String, DateTime, Time
from app.db import Base

# 1. Store status logs
class StoreStatus(Base):
    __tablename__ = "store_status"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timestamp_utc = Column(DateTime, index=True)
    status = Column(String)   # "active" / "inactive"

# 2. Business hours
class BusinessHours(Base):
    __tablename__ = "business_hours"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    day_of_week = Column(Integer)  # 0=Monday, 6=Sunday
    start_time_local = Column(Time)
    end_time_local = Column(Time)

# 3. Store timezone
class StoreTimezone(Base):
    __tablename__ = "store_timezone"
    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(String, index=True)
    timezone_str = Column(String)  # e.g. "America/Chicago"
