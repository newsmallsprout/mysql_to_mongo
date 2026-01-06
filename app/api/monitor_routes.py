from fastapi import APIRouter, HTTPException, BackgroundTasks
from app.monitor.models import MonitorConfig
from app.monitor.store import load_monitor_config, save_monitor_config
from app.monitor.engine import monitor_engine

router = APIRouter()

@router.get("/status")
def get_monitor_status():
    return monitor_engine.get_status()

@router.get("/config")
def get_monitor_config():
    cfg = load_monitor_config()
    # Mask sensitive fields so they must be re-entered on save
    cfg.es_password = ""
    cfg.slack_webhook_url = ""
    return cfg

@router.post("/config")
def update_monitor_config(cfg: MonitorConfig):
    # Enforce re-entry of sensitive fields
    if not cfg.slack_webhook_url:
        raise HTTPException(status_code=400, detail="Slack Webhook URL is required (please re-enter)")
    if not cfg.es_password:
        raise HTTPException(status_code=400, detail="Elasticsearch Password is required (please re-enter)")
        
    save_monitor_config(cfg)
    # Restart if running to apply changes
    monitor_engine.restart()
    return {"status": "ok", "message": "Config updated and monitor restarted"}

@router.post("/start")
def start_monitor():
    monitor_engine.start()
    return {"status": "ok", "message": "Monitor started"}

@router.post("/stop")
def stop_monitor():
    monitor_engine.stop()
    return {"status": "ok", "message": "Monitor stopped"}
