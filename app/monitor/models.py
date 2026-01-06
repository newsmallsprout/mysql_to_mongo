from pydantic import BaseModel, Field
from typing import List, Optional

class MonitorConfig(BaseModel):
    enabled: bool = False
    es_hosts: str = "https://localhost:9200"
    es_username: str = "elastic"
    es_password: str = ""
    index_pattern: str = "trace_log-*"
    slack_webhook_url: str = ""
    poll_interval_seconds: int = 60
    window_overlap_seconds: int = 5
    dedupe_ttl_seconds: int = 3600
    es_batch_size: int = 500
    alert_keywords: List[str] = Field(default_factory=lambda: [
        "发送CreateTrade消息失败",
        "Cause: com.mysql.cj.jdbc.exceptions"
    ])
    record_only_keywords: List[str] = Field(default_factory=list)
    ignore_keywords: List[str] = Field(default_factory=lambda: [
        "[Gateway Error] 请求处理异常"
    ])
