import uuid
import os
from fastapi import APIRouter, BackgroundTasks
from app.services.report_service import generate_report

router = APIRouter()

# In-memory store for report status
report_status = {}

@router.post("/trigger_report")
def trigger_report(background_tasks: BackgroundTasks):
    report_id = str(uuid.uuid4())
    output_path = f"output/report_{report_id}.csv"

    # mark as running
    report_status[report_id] = {"status": "Running", "file": None}

    # Run report in background
    background_tasks.add_task(run_report_task, report_id, output_path)

    return {"report_id": report_id}

def run_report_task(report_id: str, output_path: str):
    try:
        path = generate_report(output_path)
        report_status[report_id] = {"status": "Complete", "file": path}
    except Exception as e:
        report_status[report_id] = {"status": "Failed", "error": str(e)}

@router.get("/get_report/{report_id}")
def get_report(report_id: str):
    if report_id not in report_status:
        return {"error": "Invalid report_id"}

    status_info = report_status[report_id]
    if status_info["status"] == "Running":
        return {"status": "Running"}
    elif status_info["status"] == "Complete":
        return {
            "status": "Complete",
            "file": status_info["file"]
        }
    else:
        return {"status": "Failed", "error": status_info.get("error", "Unknown error")}
