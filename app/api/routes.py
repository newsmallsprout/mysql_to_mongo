# app/api/routes.py
from fastapi import APIRouter, HTTPException, Response, Request, Body
import hashlib
import json
import time
from time import perf_counter
import pymysql
import ssl as _ssl
from pymongo import MongoClient
from app.api.models import SyncTaskRequest, ConnectionConfig, DBConfig
from app.sync.task_manager import task_manager
from app.core.connection_store import connection_store
from app.core.uri import build_mongo_uri
from app.monitor.engine import monitor_engine
from app.monitor.store import save_monitor_config
import os
from typing import Optional, Dict, Any

router = APIRouter()

def _mysql_connect_with_fallback(kwargs):
    try:
        return pymysql.connect(**kwargs)
    except Exception as e1:
        msg = str(e1)
        if "require_secure_transport" in msg or "3159" in msg or "secure transport" in msg or "Bad handshake" in msg or "1043" in msg:
            last = e1
            for ssl_opt in ({}, {"fake_flag_to_enable_tls": True}, {"check_hostname": False, "verify_mode": _ssl.CERT_NONE}):
                try:
                    kw = dict(kwargs)
                    kw["ssl"] = ssl_opt
                    return pymysql.connect(**kw)
                except Exception as e2:
                    last = e2
            raise last
        raise

def _get_monitor_task_status():
    ms = monitor_engine.get_status()
    # Format as task
    return {
        "task_id": "monitor",
        "status": ms["status"],
        "config": {
            "mysql": "Elasticsearch", # Source
            "mongo": "Slack",         # Target
            "tables": [ms["config"].get("index_pattern", "-")]
        },
        "metrics": {
            "phase": "Monitoring",
            "processed_count": ms["alerts_sent"],
            "error_count": ms.get("levels", {}).get("error", 0),
            "warn_count": ms.get("levels", {}).get("warn", 0),
            "info_count": ms.get("levels", {}).get("info", 0),
            "other_count": ms.get("levels", {}).get("other", 0),
            "binlog_file": "Last Run",
            "binlog_pos": ms["last_run"] or "-",
            "last_update": time.time(), # Approximate
            "error": ms["last_error"]
        },
        "type": "monitor" # Marker for frontend
    }

@router.get("/connections")
def list_connections():
    return {"connections": connection_store.list_all()}


@router.post("/connections")
def save_connection(conn: ConnectionConfig):
    if not conn.password or not conn.password.strip():
        raise HTTPException(status_code=400, detail="Password is required")
    connection_store.save(conn.id, conn.dict())
    return {"msg": "saved", "id": conn.id}


@router.get("/connections/{conn_id}")
def get_connection(conn_id: str):
    cfg = connection_store.load(conn_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Connection not found")
    if isinstance(cfg, dict) and "password" in cfg:
        cfg = {k: v for k, v in cfg.items() if k != "password"}
    return cfg


@router.delete("/connections/{conn_id}")
def delete_connection(conn_id: str):
    connection_store.delete(conn_id)
    return {"msg": "deleted", "id": conn_id}

def _test_mysql_conn(cfg: ConnectionConfig) -> int:
    try:
        start = perf_counter()
        kwargs = dict(
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
            conn = _mysql_connect_with_fallback(kwargs)
        except Exception:
            raise
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
        kwargs = dict(
            host=conn.host,
            port=int(conn.port),
            user=conn.user,
            passwd=conn.password,
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
        )
        try:
            c = _mysql_connect_with_fallback(kwargs)
        except Exception:
            raise
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

@router.post("/mysql/databases_by_id/{conn_id}")
def list_mysql_databases_by_id(conn_id: str):
    cfg = connection_store.load(conn_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Connection not found")
    if isinstance(cfg, dict) and cfg.get("type") != "mysql":
        raise HTTPException(status_code=400, detail="Expect mysql connection")
    try:
        kwargs = dict(
            host=cfg.get("host"),
            port=int(cfg.get("port")),
            user=cfg.get("user"),
            passwd=cfg.get("password"),
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
        )
        try:
            c = _mysql_connect_with_fallback(kwargs)
        except Exception:
            raise
        try:
            with c.cursor() as cur:
                cur.execute("SHOW DATABASES")
                rows = [r[0] for r in cur.fetchall()]
        finally:
            c.close()
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
        kwargs = dict(
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
            c = _mysql_connect_with_fallback(kwargs)
        except Exception:
            raise
        try:
            with c.cursor() as cur:
                cur.execute("SHOW TABLES")
                rows = [r[0] for r in cur.fetchall()]
        finally:
            c.close()
        return {"tables": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"List tables failed: {str(e)[:200]}")

@router.post("/mysql/tables_by_id/{conn_id}")
def list_mysql_tables_by_id(conn_id: str, payload: dict = Body(...)):
    cfg = connection_store.load(conn_id)
    if not cfg:
        raise HTTPException(status_code=404, detail="Connection not found")
    if isinstance(cfg, dict) and cfg.get("type") != "mysql":
        raise HTTPException(status_code=400, detail="Expect mysql connection")
    database = (payload or {}).get("database")
    if not database:
        raise HTTPException(status_code=400, detail="Database is required")
    try:
        kwargs = dict(
            host=cfg.get("host"),
            port=int(cfg.get("port")),
            user=cfg.get("user"),
            passwd=cfg.get("password"),
            db=database,
            connect_timeout=5,
            read_timeout=5,
            write_timeout=5,
        )
        try:
            c = _mysql_connect_with_fallback(kwargs)
        except Exception:
            raise
        try:
            with c.cursor() as cur:
                cur.execute("SHOW TABLES")
                rows = [r[0] for r in cur.fetchall()]
        finally:
            c.close()
        return {"tables": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"List tables failed: {str(e)[:200]}")

@router.get("/tasks/list")
def list_tasks_route():
    return {"tasks": task_manager.list_tasks()}


@router.get("/tasks/status")
def get_tasks_status():
    tasks = task_manager.get_all_tasks_status()
    # Append Monitor Task
    tasks.append(_get_monitor_task_status())
    return {"tasks": tasks}


@router.post("/tasks/start")
def start_task(cfg: SyncTaskRequest):
    task_manager.start(cfg)
    return {"msg": "started", "task_id": cfg.task_id}

@router.post("/tasks/start_with_conn_ids")
def start_task_with_conn_ids(payload: Dict[str, Any] = Body(...)):
    try:
        task_id = payload.get("task_id")
        if not task_id:
            raise HTTPException(status_code=400, detail="task_id is required")
        source_conn_id: Optional[str] = payload.get("source_conn_id")
        target_conn_id: Optional[str] = payload.get("target_conn_id")
        mysql_db: Optional[str] = payload.get("mysql_database")
        mongo_db_override: Optional[str] = payload.get("mongo_database")
        table_map: Dict[str, str] = payload.get("table_map") or {}
        pk_field: str = payload.get("pk_field") or "id"
        update_insert_new_doc: bool = bool(payload.get("update_insert_new_doc"))
        delete_append_new_doc: bool = bool(payload.get("delete_append_new_doc"))
        auto_discover_new_tables: bool = bool(payload.get("auto_discover_new_tables"))

        if not mysql_db:
            raise HTTPException(status_code=400, detail="mysql_database is required")

        # Build MySQL DBConfig (prefer saved connection if provided)
        mysql_conf: Optional[DBConfig] = None
        if source_conn_id:
            src = connection_store.load(source_conn_id)
            if not src:
                raise HTTPException(status_code=404, detail="Source connection not found")
            if src.get("type") != "mysql":
                raise HTTPException(status_code=400, detail="Source connection must be mysql")
            mysql_conf = DBConfig(
                host=src.get("host"),
                port=int(src.get("port")),
                user=src.get("user"),
                password=src.get("password"),
                database=mysql_db,
            )
        else:
            override = payload.get("mysql_conf_override") or {}
            try:
                mysql_conf = DBConfig(
                    host=override.get("host"),
                    port=int(override.get("port")),
                    user=override.get("user"),
                    password=override.get("password"),
                    database=mysql_db,
                )
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid mysql_conf_override")

        # Build Mongo DBConfig (prefer saved connection if provided)
        mongo_conf: Optional[DBConfig] = None
        if target_conn_id:
            tgt = connection_store.load(target_conn_id)
            if not tgt:
                raise HTTPException(status_code=404, detail="Target connection not found")
            if tgt.get("type") != "mongo":
                raise HTTPException(status_code=400, detail="Target connection must be mongo")
            base_db = mongo_db_override or tgt.get("database") or "sync_db"
            if tgt.get("hosts"):
                mongo_conf = DBConfig(
                    hosts=tgt.get("hosts"),
                    replica_set=tgt.get("replica_set"),
                    user=tgt.get("user"),
                    password=tgt.get("password"),
                    database=base_db,
                )
            else:
                mongo_conf = DBConfig(
                    host=tgt.get("host"),
                    port=int(tgt.get("port") or 27017),
                    user=tgt.get("user"),
                    password=tgt.get("password"),
                    database=base_db,
                )
        else:
            override = payload.get("mongo_conf_override") or {}
            try:
                base_db = mongo_db_override or override.get("database") or "sync_db"
                if override.get("hosts"):
                    mongo_conf = DBConfig(
                        hosts=override.get("hosts"),
                        replica_set=override.get("replica_set"),
                        user=override.get("user"),
                        password=override.get("password"),
                        database=base_db,
                    )
                else:
                    mongo_conf = DBConfig(
                        host=override.get("host"),
                        port=int(override.get("port") or 27017),
                        user=override.get("user"),
                        password=override.get("password"),
                        database=base_db,
                    )
            except Exception:
                raise HTTPException(status_code=400, detail="Invalid mongo_conf_override")

        # Compose SyncTaskRequest
        req = SyncTaskRequest(
            task_id=task_id,
            mysql_conf=mysql_conf,
            mongo_conf=mongo_conf,
            table_map=table_map,
            pk_field=pk_field,
            update_insert_new_doc=update_insert_new_doc,
            insert_only=False,
            handle_deletes=True,
            auto_discover_new_tables=auto_discover_new_tables,
        )
        task_manager.start(req)
        return {"msg": "started", "task_id": task_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Start with conn ids failed: {str(e)[:200]}")

@router.post("/tasks/start_existing/{task_id}")
def start_existing(task_id: str):
    if task_id == "monitor":
        monitor_engine.start()
        return {"msg": "started_existing", "task_id": task_id}
        
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
    if task_id == "monitor":
        monitor_engine.stop()
        return {"msg": "stopped", "task_id": task_id}

    task_manager.stop(task_id)
    return {"msg": "stopped", "task_id": task_id}


@router.post("/tasks/stop_soft/{task_id}")
def stop_task_soft(task_id: str):
    if task_id == "monitor":
        monitor_engine.stop()
        return {"msg": "stopped_soft", "task_id": task_id}
        
    task_manager.stop_soft(task_id)
    return {"msg": "stopped_soft", "task_id": task_id}


@router.post("/tasks/delete/{task_id}")
def delete_task(task_id: str):
    if task_id == "monitor":
        monitor_engine.stop()
        # Disable in config
        cfg = monitor_engine.cfg
        cfg.enabled = False
        save_monitor_config(cfg)
        return {"msg": "deleted", "task_id": task_id}

    task_manager.delete(task_id)
    return {"msg": "deleted", "task_id": task_id}


@router.post("/tasks/reset/{task_id}")
def reset_task(task_id: str):
    if task_id == "monitor":
        # Monitor doesn't support reset (maybe clear state?)
        return {"msg": "reset", "task_id": task_id}

    task_manager.reset(task_id)
    return {"msg": "reset", "task_id": task_id}

@router.post("/tasks/reset_and_start/{task_id}")
def reset_and_start(task_id: str):
    if task_id == "monitor":
        monitor_engine.restart()
        return {"msg": "reset_and_started", "task_id": task_id}

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
    if task_id == "monitor":
        item = _get_monitor_task_status()
        metrics_str = json.dumps(item.get("metrics"), sort_keys=True, ensure_ascii=False)
        etag = f'W/"{hashlib.md5(metrics_str.encode()).hexdigest()}"'
        if request.headers.get("if-none-match") == etag:
            return Response(status_code=304)
        response.headers["ETag"] = etag
        return item

    for item in task_manager.get_all_tasks_status():
        if item.get("task_id") == task_id:
            metrics_str = json.dumps(item.get("metrics"), sort_keys=True, ensure_ascii=False)
            etag = f'W/"{hashlib.md5(metrics_str.encode()).hexdigest()}"'
            if request.headers.get("if-none-match") == etag:
                return Response(status_code=304)
            response.headers["ETag"] = etag
            return item
    raise HTTPException(status_code=404, detail="Task not running")


@router.get("/tasks/logs/{task_id}/download")
def download_task_logs(task_id: str, keyword: str = "", start_time: str = "", end_time: str = ""):
    p = os.path.join("logs", f"{task_id}.log")
    if not os.path.exists(p):
        return Response(content="", media_type="text/plain")

    try:
        def iter_logs():
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    # 1. Keyword Filter
                    if keyword and keyword.lower() not in line.lower():
                        continue
                    
                    # 2. Time Range Filter (Expects format [YYYY-MM-DD HH:MM:SS])
                    if start_time or end_time:
                        try:
                            # Extract timestamp from log line (assuming standard format)
                            # Example: [2023-01-01 12:00:00] [task] msg...
                            if line.startswith("["):
                                rb = line.find("]")
                                if rb > 1:
                                    ts_str = line[1:rb]
                                    # Simple string comparison works for ISO-like dates
                                    if start_time and ts_str < start_time:
                                        continue
                                    if end_time and ts_str > end_time:
                                        continue
                        except:
                            pass # Skip time check if parse fails
                    
                    yield line

        from fastapi.responses import StreamingResponse
        filename = f"{task_id}_logs_{int(time.time())}.txt"
        return StreamingResponse(
            iter_logs(),
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download logs failed: {str(e)[:200]}")

@router.get("/tasks/logs/{task_id}")
def get_task_logs(task_id: str, page: int = 1, page_size: int = 100):
    p = os.path.join("logs", f"{task_id}.log")
    if not os.path.exists(p):
        return {"lines": [], "total": 0, "page": 1, "page_size": page_size}
    try:
        with open(p, "r", encoding="utf-8") as f:
            all_lines = f.readlines()
        
        total = len(all_lines)
        page_size = max(1, min(page_size, 2000)) # Cap at 2000 lines per page
        
        # Handle -1 for last page
        if page == -1:
            import math
            page = max(1, math.ceil(total / page_size))
        else:
            page = max(1, page)
        
        start = (page - 1) * page_size
        end = start + page_size
        
        return {
            "lines": all_lines[start:end],
            "total": total,
            "page": page,
            "page_size": page_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Read logs failed: {str(e)[:200]}")
