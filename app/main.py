import os
import glob
import json
import time
import threading
import random
from datetime import date, datetime as dt
from collections import Counter
from typing import Optional, Dict, List, Any, Tuple
import ssl as _ssl

import pymysql
from pymysql.cursors import SSDictCursor

from decimal import Decimal, ROUND_DOWN
from bson.decimal128 import Decimal128

from pymongo import MongoClient, InsertOne, DeleteOne
from pymongo.write_concern import WriteConcern
from pymongo.errors import BulkWriteError, AutoReconnect, OperationFailure

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent

from fastapi import FastAPI
from pydantic import BaseModel, Field
from urllib.parse import quote_plus


# =========================
# 日志输出（避免输出大对象）
# =========================
def log(task_id: str, msg: str):
    print(f"[{task_id}] {msg}", flush=True)


app = FastAPI(title="MySQL to Mongo Syncer (Performance Optimized)")

STATE_DIR = "state"
TASK_CONFIG_DIR = "configs"
os.makedirs(STATE_DIR, exist_ok=True)
os.makedirs(TASK_CONFIG_DIR, exist_ok=True)

_active_tasks_lock = threading.Lock()
active_tasks: Dict[str, "SyncWorker"] = {}


# =========================
# 配置模型
# =========================
class DBConfig(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    user: str
    password: str
    database: Optional[str] = None

    # Mongo
    replica_set: Optional[str] = None
    hosts: Optional[List[str]] = None
    auth_source: Optional[str] = "admin"

    # MySQL
    use_ssl: bool = True
    ssl_ca: Optional[str] = None  # CA 证书路径（推荐）
    ssl_cert: Optional[str] = None  # 客户端证书（可选）
    ssl_key: Optional[str] = None  # 客户端私钥（可选）
    ssl_verify_cert: bool = False  # 是否校验证书（生产建议 True）
    charset: str = "utf8mb4"


class SyncTaskRequest(BaseModel):
    task_id: str
    mysql_conf: DBConfig
    mongo_conf: DBConfig

    # mysql_table -> mongo_collection
    table_map: Dict[str, str] = Field(default_factory=dict)

    pk_field: str = "id"
    collection_suffix: str = ""

    # 日志输出间隔（秒）
    progress_interval: int = 10

    # 全量同步：MySQL 拉取批大小 / Mongo 批写大小
    mysql_fetch_batch: int = 2000
    mongo_bulk_batch: int = 2000

    # 增量同步：批写大小 / 最大等待时间（秒）
    inc_flush_batch: int = 2000
    inc_flush_interval_sec: int = 2

    # 状态落盘节流（秒）
    state_save_interval_sec: int = 2

    # 行为策略
    insert_only: bool = False               # True：只消费插入事件
    handle_updates_as_insert: bool = True # update 当 insert 追加（仅 insert_only=False 时有效）
    handle_deletes: bool = True           # 允许删除（仅 insert_only=False 且 use_pk_as_mongo_id=True 时建议开启）

    # 去重策略：将 MySQL 主键写入 Mongo _id（推荐开启）
    use_pk_as_mongo_id: bool = True

    # Mongo 连接池
    mongo_max_pool_size: int = 50

    # Mongo 写入一致性（性能优先：w=1,j=False）
    mongo_write_w: int = 1
    mongo_write_j: bool = False

    # Mongo socket 超时（避免网络抖动阻塞线程）
    mongo_socket_timeout_ms: int = 20000
    mongo_connect_timeout_ms: int = 10000

    # MySQL 连接超时（避免阻塞）
    mysql_connect_timeout: int = 10
    mysql_read_timeout: int = 60
    mysql_write_timeout: int = 60


# =========================
# 配置与状态持久化
# =========================
def save_task_config(config: SyncTaskRequest):
    with open(os.path.join(TASK_CONFIG_DIR, f"{config.task_id}.json"), "w", encoding="utf-8") as f:
        f.write(config.model_dump_json())


def delete_task_config(task_id: str):
    p = os.path.join(TASK_CONFIG_DIR, f"{task_id}.json")
    if os.path.exists(p):
        os.remove(p)


def load_state(task_id: str):
    p = os.path.join(STATE_DIR, f"{task_id}.json")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_state(task_id: str, log_file: str, log_pos: int):
    with open(os.path.join(STATE_DIR, f"{task_id}.json"), "w", encoding="utf-8") as f:
        json.dump({"log_file": log_file, "log_pos": log_pos}, f)


# =========================
# Mongo URI 构建
# =========================
def build_mongo_uri(m: DBConfig) -> str:
    u = quote_plus(m.user)
    p = quote_plus(m.password)
    db = m.database or "sync_db"

    if m.hosts and m.replica_set:
        hosts = ",".join(m.hosts)
        rs = quote_plus(m.replica_set)
        return (
            f"mongodb://{u}:{p}@{hosts}/{db}"
            f"?replicaSet={rs}&retryWrites=true"
            f"&authSource=admin&authMechanism=SCRAM-SHA-256"
        )

    host = m.host or "localhost"
    port = int(m.port or 27017)
    return (
        f"mongodb://{u}:{p}@{host}:{port}/{db}"
        f"?retryWrites=true&authSource=admin&authMechanism=SCRAM-SHA-256"
    )


# =========================
# 同步工作线程
# =========================
class SyncWorker:
    DEC_SCALE = 18
    DEC_Q = Decimal("1").scaleb(-DEC_SCALE)

    def __init__(self, cfg: SyncTaskRequest):
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.stream: Optional[BinLogStreamReader] = None

        # MySQL 连接参数（性能与可控超时）
        self.mysql_settings = {
            "host": cfg.mysql_conf.host,
            "port": int(cfg.mysql_conf.port or 3306),
            "user": cfg.mysql_conf.user,
            "passwd": cfg.mysql_conf.password,
            "db": cfg.mysql_conf.database,
            "charset": cfg.mysql_conf.charset,
            "cursorclass": SSDictCursor,
            "connect_timeout": int(cfg.mysql_connect_timeout or 10),
            "read_timeout": int(cfg.mysql_read_timeout or 60),
            "write_timeout": int(cfg.mysql_write_timeout or 60),
        }
        if cfg.mysql_conf.use_ssl:
            ssl_args = {}
            if cfg.mysql_conf.ssl_ca:
                ssl_args["ca"] = cfg.mysql_conf.ssl_ca
            if cfg.mysql_conf.ssl_cert:
                ssl_args["cert"] = cfg.mysql_conf.ssl_cert
            if cfg.mysql_conf.ssl_key:
                ssl_args["key"] = cfg.mysql_conf.ssl_key

            # 如果你没有 CA，又必须连（仅建议内网测试）
            if not cfg.mysql_conf.ssl_verify_cert:
                ssl_args["check_hostname"] = False
                ssl_args["verify_mode"] = _ssl.CERT_NONE
            else:
                # 有 CA 时建议默认校验
                ssl_args["verify_mode"] = _ssl.CERT_REQUIRED

            self.mysql_settings["ssl"] = ssl_args

        # Mongo 客户端（连接池与超时设置）
        mongo_uri = build_mongo_uri(cfg.mongo_conf)
        self.mongo = MongoClient(
            mongo_uri,
            maxPoolSize=int(cfg.mongo_max_pool_size or 50),
            minPoolSize=0,
            connect=False,
            retryWrites=True,
            socketTimeoutMS=int(cfg.mongo_socket_timeout_ms or 20000),
            connectTimeoutMS=int(cfg.mongo_connect_timeout_ms or 10000),
        )
        self.mongo_db = self.mongo[cfg.mongo_conf.database or "sync_db"]

        # 常用字段预计算
        self._pk_lower = cfg.pk_field.lower()

        # state 落盘节流
        self._last_state_save_ts = 0.0

        # 进度日志节流
        self._last_progress_ts = 0.0

    # -------------------------
    # 停止线程
    # -------------------------
    def stop(self):
        self.stop_event.set()
        try:
            if self.stream is not None:
                self.stream.close()
        except Exception:
            pass

    # -------------------------
    # 类型转换（Decimal/日期等）
    # -------------------------
    def _convert_value(self, obj: Any):
        if isinstance(obj, Decimal):
            dq = obj.quantize(self.DEC_Q, rounding=ROUND_DOWN)
            return Decimal128(dq)
        if isinstance(obj, dt):
            return obj
        if isinstance(obj, date):
            return dt(obj.year, obj.month, obj.day)
        if isinstance(obj, dict):
            return {k: self._convert_value(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._convert_value(v) for v in obj]
        return obj

    def _row_to_doc(self, row: dict) -> dict:
        doc = {}
        for k, v in row.items():
            if isinstance(v, Decimal):
                dq = v.quantize(self.DEC_Q, rounding=ROUND_DOWN)
                doc[k] = Decimal128(dq)
                doc[f"{k}_str"] = format(dq, "f")
            else:
                doc[k] = self._convert_value(v)

        # 去重策略：主键写入 _id（建议开启）
        if self.cfg.use_pk_as_mongo_id:
            for kk, vv in row.items():
                if kk.lower() == self._pk_lower:
                    doc["_id"] = vv
                    break

        return doc

    # -------------------------
    # 自动构建 table_map（未提供时从 MySQL 读取）
    # -------------------------
    def _auto_build_table_map_if_needed(self):
        if self.cfg.table_map:
            return

        conn = pymysql.connect(**{k: v for k, v in self.mysql_settings.items() if k != "cursorclass"})
        try:
            with conn.cursor() as c:
                c.execute("SHOW FULL TABLES WHERE Table_type='BASE TABLE'")
                tables = [r[0] for r in c.fetchall()]
                self.cfg.table_map = {t: t + self.cfg.collection_suffix for t in tables}
        finally:
            conn.close()

    # -------------------------
    # BulkWriteError 摘要（不输出 op）
    # -------------------------
    def _summarize_bulk_error(self, table: str, coll_name: str, e: BulkWriteError, max_samples: int = 3) -> List[dict]:
        details = e.details or {}
        write_errors = details.get("writeErrors", []) or []

        code_counter = Counter()
        samples = []
        for w in write_errors[:max_samples]:
            code = w.get("code")
            msg = (w.get("errmsg") or "")[:180]
            idx = w.get("index")
            code_counter[code] += 1
            samples.append(f"idx={idx} code={code} msg={msg}")

        log(
            self.cfg.task_id,
            f"BulkWriteError t={table} c={coll_name} "
            f"errors={len(write_errors)} codes={dict(code_counter)} samples={samples}"
        )
        return write_errors

    # -------------------------
    # 安全批量写：指数退避 + 可忽略重复键
    # -------------------------
    def _safe_bulk_write(self, coll, ops: List, table: str, coll_name: str, max_retry: int = 6) -> bool:
        if not ops:
            return True

        backoff = 1.0
        for _ in range(max_retry):
            if self.stop_event.is_set():
                return False

            try:
                coll.bulk_write(ops, ordered=False)
                return True

            except BulkWriteError as e:
                write_errors = self._summarize_bulk_error(table, coll_name, e)

                # 11000：重复键（多见于 _id 去重），通常可忽略
                only_dup = (len(write_errors) > 0) and all(w.get("code") == 11000 for w in write_errors)
                if only_dup:
                    return True

                # 215：库/集合处于 drop 过程中，退避重试
                has_215 = any(w.get("code") == 215 for w in write_errors)
                if not has_215:
                    return False

            except (AutoReconnect, OperationFailure) as e:
                # 仅输出简短信息，避免大量堆栈与对象打印
                log(self.cfg.task_id, f"Mongo transient error: {type(e).__name__}: {str(e)[:180]}")

            time.sleep(min(30.0, backoff) + random.random() * 0.2)
            backoff *= 2

        log(self.cfg.task_id, f"Mongo write failed after retries t={table} c={coll_name} batch={len(ops)}")
        return False

    # -------------------------
    # state 落盘节流
    # -------------------------
    def _maybe_save_state(self, log_file: str, log_pos: int):
        now = time.time()
        interval = max(1, int(self.cfg.state_save_interval_sec or 2))
        if now - self._last_state_save_ts >= interval:
            save_state(self.cfg.task_id, log_file, log_pos)
            self._last_state_save_ts = now

    # -------------------------
    # 进度日志节流
    # -------------------------
    def _maybe_progress_log(self, msg: str):
        now = time.time()
        interval = max(1, int(self.cfg.progress_interval or 10))
        if now - self._last_progress_ts >= interval:
            log(self.cfg.task_id, msg)
            self._last_progress_ts = now

    # =========================
    # 入口：决定全量/增量
    # =========================
    def run(self):
        log(self.cfg.task_id, "Task started")
        try:
            self._auto_build_table_map_if_needed()
            state = load_state(self.cfg.task_id)

            if not state:
                self.do_full_sync()
                # 全量完成后，从当前位置开始增量（没有 state 时传 None）
                self.do_inc_sync(None, None)
            else:
                self.do_inc_sync(state.get("log_file"), state.get("log_pos"))

        except Exception as e:
            log(self.cfg.task_id, f"CRASH {type(e).__name__}: {str(e)[:300]}")

    # =========================
    # 全量同步：按主键分页 + bulk_write
    # =========================
    def do_full_sync(self):
        mysql_batch = int(self.cfg.mysql_fetch_batch or 2000)
        mongo_batch = int(self.cfg.mongo_bulk_batch or 2000)
        pk = self.cfg.pk_field

        write_concern = WriteConcern(w=int(self.cfg.mongo_write_w or 1), j=bool(self.cfg.mongo_write_j))

        conn = pymysql.connect(**self.mysql_settings)
        try:
            with conn.cursor() as c:
                for table, coll_name in self.cfg.table_map.items():
                    if self.stop_event.is_set():
                        break

                    coll = self.mongo_db.get_collection(coll_name, write_concern=write_concern)

                    processed = 0
                    last_id = None
                    ops: List[InsertOne] = []
                    start = time.time()

                    log(self.cfg.task_id, f"FullSync table={table} -> collection={coll_name}")

                    while not self.stop_event.is_set():
                        if last_id is None:
                            c.execute(
                                f"SELECT * FROM `{table}` ORDER BY `{pk}` LIMIT %s",
                                (mysql_batch,),
                            )
                        else:
                            c.execute(
                                f"SELECT * FROM `{table}` WHERE `{pk}` > %s ORDER BY `{pk}` LIMIT %s",
                                (last_id, mysql_batch),
                            )

                        rows = c.fetchall()
                        if not rows:
                            break

                        for r in rows:
                            # 要求主键严格递增（适用于数值/可比较的主键）
                            if pk in r:
                                last_id = r[pk]
                            doc = self._row_to_doc(r)
                            ops.append(InsertOne(doc))
                            processed += 1

                            if len(ops) >= mongo_batch:
                                self._safe_bulk_write(coll, ops, table, coll_name)
                                ops.clear()

                        elapsed = max(1e-6, time.time() - start)
                        speed = int(processed / elapsed)
                        self._maybe_progress_log(f"FullSync prog table={table} done={processed} speed={speed} row/s")

                    if ops:
                        self._safe_bulk_write(coll, ops, table, coll_name)
                        ops.clear()

                    elapsed = max(1e-6, time.time() - start)
                    speed = int(processed / elapsed)
                    log(self.cfg.task_id, f"FullSync done table={table} inserted={processed} speed={speed} row/s")

        finally:
            conn.close()

    # =========================
    # 增量同步：聚合批量写 + 节流保存 state
    # =========================
    def do_inc_sync(self, log_file, log_pos):
        write_concern = WriteConcern(w=int(self.cfg.mongo_write_w or 1), j=bool(self.cfg.mongo_write_j))

        # 事件订阅策略
        if self.cfg.insert_only:
            only_events = [WriteRowsEvent]
        else:
            only_events = [WriteRowsEvent]
            if self.cfg.handle_updates_as_insert:
                only_events.append(UpdateRowsEvent)
            if self.cfg.handle_deletes:
                only_events.append(DeleteRowsEvent)

        self.stream = BinLogStreamReader(
            connection_settings={k: v for k, v in self.mysql_settings.items() if k != "cursorclass"},
            server_id=100 + int(time.time() % 100) + random.randint(0, 1000),
            log_file=log_file,
            log_pos=log_pos,
            blocking=True,
            resume_stream=True,
            only_events=only_events,
        )

        inc_batch = int(self.cfg.inc_flush_batch or 2000)
        flush_interval = max(1, int(self.cfg.inc_flush_interval_sec or 2))

        # 每个集合累积一批操作，减少频繁写入
        pending: Dict[str, List] = {}
        last_flush_ts = time.time()

        log(self.cfg.task_id, f"IncSync started events={[e.__name__ for e in only_events]}")

        def flush_all(force: bool = False):
            nonlocal last_flush_ts
            now = time.time()
            if (not force) and (now - last_flush_ts < flush_interval):
                return

            for cn, ops in list(pending.items()):
                if not ops:
                    continue
                coll = self.mongo_db.get_collection(cn, write_concern=write_concern)
                self._safe_bulk_write(coll, ops, table="*", coll_name=cn)
                pending[cn].clear()

            # state 节流保存
            try:
                self._maybe_save_state(self.stream.log_file, self.stream.log_pos)
            except Exception:
                pass

            last_flush_ts = now

        try:
            for ev in self.stream:
                if self.stop_event.is_set():
                    break

                table = ev.table
                if table not in self.cfg.table_map:
                    # 非目标表忽略
                    continue

                coll_name = self.cfg.table_map[table]
                ops = pending.setdefault(coll_name, [])

                # 插入事件
                if isinstance(ev, WriteRowsEvent):
                    for row in ev.rows:
                        data = row.get("values")
                        if data:
                            ops.append(InsertOne(self._row_to_doc(data)))

                # 更新事件（可选：当 insert 追加）
                elif isinstance(ev, UpdateRowsEvent) and self.cfg.handle_updates_as_insert:
                    for row in ev.rows:
                        data = row.get("after_values")
                        if data:
                            ops.append(InsertOne(self._row_to_doc(data)))

                # 删除事件（可选：按 _id 删除）
                elif isinstance(ev, DeleteRowsEvent) and self.cfg.handle_deletes and self.cfg.use_pk_as_mongo_id:
                    for row in ev.rows:
                        data = row.get("values")
                        if not data:
                            continue
                        pk_val = None
                        for kk, vv in data.items():
                            if kk.lower() == self._pk_lower:
                                pk_val = vv
                                break
                        if pk_val is not None:
                            ops.append(DeleteOne({"_id": pk_val}))

                # 达到批大小或到时间就 flush
                if len(ops) >= inc_batch:
                    flush_all(force=True)
                else:
                    flush_all(force=False)

        finally:
            # 退出前强制 flush
            try:
                flush_all(force=True)
            except Exception:
                pass

            try:
                if self.stream is not None:
                    # 尽量落一次最终 state
                    try:
                        self._maybe_save_state(self.stream.log_file, self.stream.log_pos)
                    except Exception:
                        pass
                    self.stream.close()
            except Exception:
                pass

            log(self.cfg.task_id, "IncSync stopped")


# =========================
# 任务启动与恢复
# =========================
def run_worker_thread(cfg: SyncTaskRequest):
    w = SyncWorker(cfg)
    with _active_tasks_lock:
        active_tasks[cfg.task_id] = w
    threading.Thread(target=w.run, daemon=True).start()


@app.on_event("startup")
def startup_restore_tasks():
    for p in glob.glob(os.path.join(TASK_CONFIG_DIR, "*.json")):
        try:
            with open(p, encoding="utf-8") as f:
                cfg = SyncTaskRequest(**json.load(f))
            run_worker_thread(cfg)
        except Exception as e:
            print(f"[startup] restore failed {p}: {type(e).__name__}: {str(e)[:200]}", flush=True)


# =========================
# API
# =========================
@app.post("/tasks/start")
def start_task(cfg: SyncTaskRequest):
    save_task_config(cfg)
    run_worker_thread(cfg)
    return {"msg": "started", "task_id": cfg.task_id}


@app.post("/tasks/stop/{task_id}")
def stop_task(task_id: str):
    delete_task_config(task_id)
    with _active_tasks_lock:
        w = active_tasks.get(task_id)
        if w is not None:
            w.stop()
            del active_tasks[task_id]
    return {"msg": "stopped", "task_id": task_id}


@app.get("/")
def root():
    with _active_tasks_lock:
        return {"tasks": list(active_tasks.keys())}
