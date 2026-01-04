# app/api/routes.py
from fastapi import APIRouter, HTTPException, Response, Request
import hashlib
import json
from time import perf_counter
import pymysql
from pymongo import MongoClient
from app.api.models import SyncTaskRequest, ConnectionConfig, DBConfig
from app.sync.task_manager import task_manager
from app.core.connection_store import connection_store
from app.core.uri import build_mongo_uri
import os
import pymysql

router = APIRouter()


@router.get("/connections")
def list_connections():
    return {"connections": connection_store.list_all()}


@router.post("/connections")
def save_connection(conn: ConnectionConfig):
    connection_store.save(conn.id, conn.dict())
    return {"msg": "saved", "id": conn.id}


@router.get("/connections/{conn_id}")
def get_connection(conn_id: str):
    cfg = connection_store.load(conn_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Connection not found")
    return cfg


@router.delete("/connections/{conn_id}")
def delete_connection(conn_id: str):
    connection_store.delete(conn_id)
    return {"msg": "deleted", "id": conn_id}

def _test_mysql_conn(cfg: ConnectionConfig) -> int:
    try:
        start = perf_counter()
        conn = pymysql.connect(
            host=cfg.host,
            port=int(cfg.port),
            user=cfg.user,
            passwd=cfg.password,
            db=cfg.database or None,
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
        )
        try:
            with conn.cursor() as c:
                c.execute("SELECT 1")
        finally:
            conn.close()
        return int((perf_counter() - start) * 1000)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MySQL test failed: {str(e)[:200]}")


def _test_mongo_conn(cfg: ConnectionConfig) -> int:
    try:
        start = perf_counter()
        # Prefer hosts list (replica set) whenever provided
        if cfg.hosts:
            dbconf = DBConfig(
                hosts=cfg.hosts,
                replica_set=cfg.replica_set,
                user=cfg.user,
                password=cfg.password,
                database=cfg.database,
                auth_source=cfg.auth_source or "admin",
            )
        else:
            dbconf = DBConfig(
                host=cfg.host,
                port=cfg.port,
                user=cfg.user,
                password=cfg.password,
                database=cfg.database,
                auth_source=cfg.auth_source or "admin",
            )
        uri = build_mongo_uri(dbconf)
        client = MongoClient(
            uri,
            connectTimeoutMS=5000,
            serverSelectionTimeoutMS=5000,
            socketTimeoutMS=5000,
        )
        try:
            client.admin.command("ping")
        finally:
            client.close()
        return int((perf_counter() - start) * 1000)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"MongoDB test failed: {str(e)[:200]}")


@router.post("/connections/test")
def test_connection(conn: ConnectionConfig):
    if conn.type == "mysql":
        latency = _test_mysql_conn(conn)
    elif conn.type == "mongo":
        latency = _test_mongo_conn(conn)
    else:
        raise HTTPException(status_code=400, detail="Unknown connection type")
    return {"ok": True, "latency_ms": latency}

@router.post("/mysql/databases")
def list_mysql_databases(conn: ConnectionConfig):
    if conn.type != "mysql":
        raise HTTPException(status_code=400, detail="Expect mysql connection")
    try:
        c = pymysql.connect(
            host=conn.host,
            port=int(conn.port),
            user=conn.user,
            passwd=conn.password,
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
        )
        try:
            with c.cursor() as cur:
                cur.execute("SHOW DATABASES")
                rows = [r[0] for r in cur.fetchall()]
        finally:
            c.close()
        # filter system schemas
        filtered = [d for d in rows if d not in ("information_schema", "performance_schema", "mysql", "sys")]
        return {"databases": filtered}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"List databases failed: {str(e)[:200]}")

@router.post("/mysql/tables")
def list_mysql_tables(conn: ConnectionConfig):
    if conn.type != "mysql":
        raise HTTPException(status_code=400, detail="Expect mysql connection")
    if not conn.database:
        raise HTTPException(status_code=400, detail="Database is required")
    try:
        c = pymysql.connect(
            host=conn.host,
            port=int(conn.port),
            user=conn.user,
            passwd=conn.password,
            db=conn.database,
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
        )
        try:
            with c.cursor() as cur:
                cur.execute("SHOW TABLES")
                rows = [r[0] for r in cur.fetchall()]
        finally:
            c.close()
        return {"tables": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"List tables failed: {str(e)[:200]}")

@router.get("/")
def root():
    return {"tasks": task_manager.list_tasks()}


@router.get("/tasks/status")
def get_tasks_status():
    return {"tasks": task_manager.get_all_tasks_status()}


@router.post("/tasks/start")
def start_task(cfg: SyncTaskRequest):
    task_manager.start(cfg)
    return {"msg": "started", "task_id": cfg.task_id}

@router.post("/tasks/start_existing/{task_id}")
def start_existing(task_id: str):
    # 已存在的任务配置，从记录点位继续启动
    if task_manager.is_running(task_id):
        raise HTTPException(status_code=400, detail="Task already running")
    try:
        task_manager.start_by_id(task_id)
        return {"msg": "started_existing", "task_id": task_id}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Task config not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Start existing failed: {str(e)[:200]}")


@router.post("/tasks/stop/{task_id}")
def stop_task(task_id: str):
    task_manager.stop(task_id)
    return {"msg": "stopped", "task_id": task_id}


@router.post("/tasks/stop_soft/{task_id}")
def stop_task_soft(task_id: str):
    task_manager.stop_soft(task_id)
    return {"msg": "stopped_soft", "task_id": task_id}


@router.post("/tasks/delete/{task_id}")
def delete_task(task_id: str):
    task_manager.delete(task_id)
    return {"msg": "deleted", "task_id": task_id}


@router.post("/tasks/reset/{task_id}")
def reset_task(task_id: str):
    task_manager.reset(task_id)
    return {"msg": "reset", "task_id": task_id}

@router.post("/tasks/reset_and_start/{task_id}")
def reset_and_start(task_id: str):
    # 仅在 stopped 状态允许
    for item in task_manager.get_all_tasks_status():
        if item.get("task_id") == task_id:
            if item.get("status") != "stopped":
                raise HTTPException(status_code=400, detail="Task must be stopped to reset and start")
            break
    task_manager.reset(task_id)
    try:
        task_manager.start_by_id(task_id)
        return {"msg": "reset_and_started", "task_id": task_id}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Task config not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reset and start failed: {str(e)[:200]}")


@router.get("/tasks/status/{task_id}")
def get_task_status(task_id: str, request: Request, response: Response):
    for item in task_manager.get_all_tasks_status():
        if item.get("task_id") == task_id:
            metrics_str = json.dumps(item.get("metrics"), sort_keys=True, ensure_ascii=False)
            etag = f'W/"{hashlib.md5(metrics_str.encode()).hexdigest()}"'
            if request.headers.get("if-none-match") == etag:
                return Response(status_code=304)
            response.headers["ETag"] = etag
            return item
    raise HTTPException(status_code=404, detail="Task not running")


@router.get("/tasks/logs/{task_id}")
def get_task_logs(task_id: str, lines: int = 200):
    p = os.path.join("logs", f"{task_id}.log")
    if not os.path.exists(p):
        return {"lines": []}
    try:
        with open(p, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        lines = max(1, min(int(lines or 200), 2000))
        return {"lines": all_lines[-lines:]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Read logs failed: {str(e)[:200]}")
