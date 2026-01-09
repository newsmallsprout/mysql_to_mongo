from pydantic import BaseModel
from typing import List, Dict, Any, Optional

class InspectionRequest(BaseModel):
    prometheus_url: str = "https://prometheus.test.exc888.org"
    ark_api_key: str = "7a15ca7d-a3ec-4818-a0f4-0046cd1bcd49"
    ark_model_id: str = "doubao-seed-1-6-251015"
    ark_base_url: str = "https://ark.cn-beijing.volces.com/api/v3"

class InspectionReport(BaseModel):
    timestamp: str
    prometheus_status: str
    down_targets: List[Dict[str, Any]]
    firing_alerts: List[Dict[str, Any]]
    metrics_summary: List[Dict[str, Any]] = []
    ai_analysis: str
    report_id: Optional[str] = None
    risk_summary: Optional[Dict[str, Any]] = None
    compare_with_yesterday: Optional[Dict[str, Any]] = None
    forecast_7_15_30: Optional[Dict[str, Any]] = None
    weekly_report_id: Optional[str] = None
    monthly_report_id: Optional[str] = None
