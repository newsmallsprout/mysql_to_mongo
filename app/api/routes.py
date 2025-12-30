# app/api/routes.py
from fastapi import APIRouter
from app.api.models import SyncTaskRequest
from app.sync.task_manager import task_manager

router = APIRouter()


@router.get("/")
def root():
    return {"tasks": task_manager.list_tasks()}


@router.post("/tasks/start")
def start_task(cfg: SyncTaskRequest):
    task_manager.start(cfg)
    return {"msg": "started", "task_id": cfg.task_id}


@router.post("/tasks/stop/{task_id}")
def stop_task(task_id: str):
    task_manager.stop(task_id)
    return {"msg": "stopped", "task_id": task_id}
