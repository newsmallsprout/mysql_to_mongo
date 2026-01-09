from fastapi import APIRouter, HTTPException, Query
from typing import Literal
from app.inspection.models import InspectionRequest, InspectionReport
from app.inspection.service import inspection_service
from app.inspection import report_store

router = APIRouter()

@router.post("/run", response_model=InspectionReport)
def run_inspection(req: InspectionRequest):
    """
    Trigger a system inspection using Prometheus data and AI analysis.
    """
    return inspection_service.run_inspection(req)


@router.get("/reports")
def list_reports(kind: Literal["daily", "weekly", "monthly"] = Query(default="daily"), limit: int = Query(default=30, ge=1, le=365)):
    items = report_store.list_reports(kind, limit=limit)
    return {"kind": kind, "items": items}


@router.get("/reports/{report_id}")
def get_report(report_id: str, kind: Literal["daily", "weekly", "monthly"] = Query(default="daily")):
    data = report_store.load_report(kind, report_id)
    if not data:
        raise HTTPException(status_code=404, detail="report not found")
    return data
