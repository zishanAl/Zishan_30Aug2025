# Store Monitoring API

This project implements a **store monitoring backend** as part of a take-home assignment.  
It monitors store activity (uptime/downtime) using status logs, business hours, and timezone data, then generates reports via an API-driven trigger + poll architecture.

---
## 🚀 Features
- Ingests 3 CSVs into a SQLite database:
  - `store_status.csv` → periodic pings (active/inactive)
  - `menu_hours.csv` → business hours (per weekday)
  - `timezones.csv` → store timezones
- Computes uptime and downtime for each store:
  - Last hour (in minutes)
  - Last day (in hours)
  - Last week (in hours)
- Exposes 2 APIs:
  - `POST /trigger_report` → starts report generation and returns `report_id`
  - `GET /get_report/{report_id}` → returns report status or completed CSV file
- Reports are saved under `/output/` as CSV files.

---

## 🛠️ Tech Stack
- **Python 3.11**
- **FastAPI** (for API framework)
- **SQLAlchemy** (ORM for DB interaction)
- **SQLite** (lightweight DB)
- **Pandas** (data manipulation)
- **Uvicorn** (server)

---

## 📂 Project Structure
```bash
store_monitoring/
├── app/
│ ├── api/
│ │ └── report_api.py
│ ├── services/
│ │ └── report_service.py
│ ├── utils/
│ │ ├── load_data.py
│ │ ├── check_db.py
│ │ └── time_utils.py
│ ├── db.py
│ ├── main.py
│ └── models.py
├── data/ # input CSV files
├── output/ # generated reports
├── requirements.txt
├── README.md
└── store_monitoring.db # SQLite DB
```

---

## ⚙️ Setup Instructions

### 1. Clone repo & setup environment
```bash
git clone <your_repo_url>
cd store_monitoring
python -m venv venv
venv\Scripts\activate      # (Windows)
pip install -r requirements.txt
```

### 2. Load CSVs into database

- Place the 3 CSV files in data/:
- store_status.csv
- menu_hours.csv
- timezones.csv

Then run:
```bash
python -m app.utils.load_data
```

### 3. Run API server
```bash
uvicorn app.main:app --reload
```
Server runs at:
API: http://127.0.0.1:8000
Swagger Docs: http://127.0.0.1:8000/docs



## Usage

```bash
1. Trigger report generation

POST /trigger_report

Response:

{ "report_id": "123e4567-e89b-12d3-a456-426614174000" }

2. Poll for report
GET /get_report/{report_id}


If still running:
{ "status": "Running" }


If complete:
{
  "status": "Complete",
  "file": "output/report_<report_id>.csv"
}

```

### CSV is available in /output/.

- Sample Output
- A sample report CSV is included in /output/report.csv

- Schema:
```bash
store_id,uptime_last_hour,downtime_last_hour,uptime_last_day,downtime_last_day,uptime_last_week,downtime_last_week
```

### Assumptions

- If timezone missing → default "America/Chicago".
- If business hours missing → assume 24×7 open.


### Improvements (Future Scope)

- 1. **Database Indexing**
   - Add indexes on `store_status.store_id` and `store_status.timestamp_utc`  
   - This speeds up filtering by store and by time range.

- 2. **Pre-aggregation in SQL**
   - Instead of pulling raw logs into Python, use SQL queries with `GROUP BY` to aggregate uptime/downtime buckets directly in the DB.
   - Example: compute total active minutes per store per day in SQL, then adjust only for business hours in Python.

- 3. **Caching**
   - Load all business hours and timezones into memory once, instead of querying for each store repeatedly.

- 4. **Parallelization**
   - Split stores into batches and process in parallel using Python multiprocessing / joblib.

- 5. Switch SQLite → PostgreSQL for production workloads, Optimize interpolation using vectorized pandas/numpy operations, Use Celery / RQ for scalable background job processing.