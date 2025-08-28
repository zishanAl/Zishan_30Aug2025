from app.db import SessionLocal
from app.models import StoreStatus, BusinessHours, StoreTimezone

def check_data():
    db = SessionLocal()
    try:
        print("Store Status rows:", db.query(StoreStatus).count())
        print("Business Hours rows:", db.query(BusinessHours).count())
        print("Store Timezone rows:", db.query(StoreTimezone).count())
    finally:
        db.close()

if __name__ == "__main__":
    check_data()
