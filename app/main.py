# app/main.py
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from app.api.routes import router
from app.sync.task_manager import task_manager

app = FastAPI(title="MySQL to Mongo Syncer (Versioning + SoftDelete, Package Layout)")

app.mount("/ui", StaticFiles(directory="static"), name="static")

app.include_router(router)

@app.on_event("startup")
def startup_restore_tasks():
    # 启动时恢复 configs/ 下的任务
    task_manager.restore_from_disk()
