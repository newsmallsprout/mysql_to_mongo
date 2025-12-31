# app/api/models.py
from typing import Optional, Dict, List
from pydantic import BaseModel, Field


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
    ssl_ca: Optional[str] = None
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    ssl_verify_cert: bool = False
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

    # 全量同步
    mysql_fetch_batch: int = 2000
    mongo_bulk_batch: int = 2000

    # 增量同步
    inc_flush_batch: int = 2000
    inc_flush_interval_sec: int = 2
    state_save_interval_sec: int = 2

    # 行为策略
    insert_only: bool = False
    handle_updates_as_insert: bool = False
    handle_deletes: bool = True

    # 硬删除（你需求是软删除，所以建议设为 False）
    hard_delete: bool = False

    # 将 MySQL 主键写入 Mongo _id（base 文档用这个）
    use_pk_as_mongo_id: bool = True

    # Mongo 设置
    mongo_max_pool_size: int = 50
    mongo_write_w: int = 1
    mongo_write_j: bool = False
    mongo_socket_timeout_ms: int = 20000
    mongo_connect_timeout_ms: int = 10000

    # MySQL 设置
    mysql_connect_timeout: int = 10
    mysql_read_timeout: int = 60
    mysql_write_timeout: int = 60

    # 自动发现
    auto_discover_new_tables: bool = True
    auto_discover_interval_sec: int = 10
    auto_discover_only_base_table: bool = True

    # 重连策略
    inc_reconnect_max_retry: int = 0
    inc_reconnect_backoff_base_sec: float = 1.0
    inc_reconnect_backoff_max_sec: float = 30.0

    # UNKNOWN_COL 修复
    unknown_col_fix_enabled: bool = True
    unknown_col_schema_cache_sec: int = 30

    # 软删除设置
    delete_flag_field: str = "deleted"
    delete_time_field: str = "deleted_at"
    delete_upsert_tombstone: bool = True
    delete_append_new_doc: bool = True

    # ====== 核心需求 ======
    update_insert_new_doc: bool = True
    delete_mark_only_base_doc: bool = True

    # Debug
    debug_binlog_events: bool = False
