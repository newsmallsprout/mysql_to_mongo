# app/sync/mysql_introspector.py
import time
from typing import Dict, List, Optional, Any

import pymysql
from app.core.logging import log


class MySQLIntrospector:
    def __init__(
        self,
        task_id: str,
        mysql_settings: dict,
        pk_field: str,
        unknown_col_fix_enabled: bool,
        unknown_col_schema_cache_sec: int,
        auto_discover_only_base_table: bool,
        converter,  # Converter，用于 convert_value
    ):
        self.task_id = task_id
        self.mysql_settings = mysql_settings
        self.pk_field = pk_field
        self._pk_lower = pk_field.lower()
        self.unknown_col_fix_enabled = unknown_col_fix_enabled
        self.unknown_col_schema_cache_sec = unknown_col_schema_cache_sec
        self.auto_discover_only_base_table = auto_discover_only_base_table
        self.converter = converter

        self._table_columns_cache: Dict[str, List[str]] = {}
        self._table_columns_cache_ts: Dict[str, float] = {}
        self._pk_index_cache: Dict[str, int] = {}

    def _connect(self):
        # introspector 读取 schema 不需要 SSDictCursor
        settings = {k: v for k, v in self.mysql_settings.items() if k != "cursorclass"}
        return pymysql.connect(**settings)

    def list_tables(self) -> List[str]:
        conn = self._connect()
        try:
            with conn.cursor() as c:
                if self.auto_discover_only_base_table:
                    c.execute("SHOW FULL TABLES WHERE Table_type='BASE TABLE'")
                    rows = c.fetchall()
                    return [r[0] for r in rows]
                c.execute("SHOW TABLES")
                rows = c.fetchall()
                return [r[0] for r in rows]
        finally:
            conn.close()

    def get_table_columns(self, table: str) -> Optional[List[str]]:
        now = time.time()
        cache_sec = max(1, int(self.unknown_col_schema_cache_sec or 30))
        if table in self._table_columns_cache and now - self._table_columns_cache_ts.get(table, 0) < cache_sec:
            return self._table_columns_cache[table]

        conn = self._connect()
        try:
            with conn.cursor() as c:
                c.execute(f"SHOW COLUMNS FROM `{table}`")
                rows = c.fetchall()
                cols = [r[0] for r in rows]
                if cols:
                    self._table_columns_cache[table] = cols
                    self._table_columns_cache_ts[table] = now
                    try:
                        pk_idx = [cc.lower() for cc in cols].index(self._pk_lower)
                        self._pk_index_cache[table] = pk_idx
                    except ValueError:
                        self._pk_index_cache.pop(table, None)
                return cols
        finally:
            conn.close()

    def fix_unknown_cols(self, table: str, row: dict) -> dict:
        if not self.unknown_col_fix_enabled:
            return row
        cols = self.get_table_columns(table)
        if not cols:
            return row
        new_row = {}
        for k, v in row.items():
            if isinstance(k, str) and k.startswith("UNKNOWN_COL"):
                try:
                    idx = int(k.replace("UNKNOWN_COL", ""))
                    if 0 <= idx < len(cols):
                        new_row[cols[idx]] = v
                    else:
                        new_row[k] = v
                except Exception:
                    new_row[k] = v
            else:
                new_row[k] = v
        return new_row

    def maybe_fix_row_unknown_cols(self, table: str, data: Optional[dict]) -> Optional[dict]:
        if not data or not self.unknown_col_fix_enabled:
            return data
        for k in data.keys():
            if isinstance(k, str) and k.startswith("UNKNOWN_COL"):
                return self.fix_unknown_cols(table, data)
        return data

    def extract_pk(self, table: str, data: dict) -> Optional[Any]:
        # 1) 正常字段名找
        for kk, vv in data.items():
            if isinstance(kk, str) and kk.lower() == self._pk_lower:
                return self.converter.convert_value(vv)

        # 2) UNKNOWN_COL + pk index 兜底
        pk_idx = self._pk_index_cache.get(table)
        if pk_idx is None:
            self.get_table_columns(table)
            pk_idx = self._pk_index_cache.get(table)
        if pk_idx is None:
            return None

        for kk, vv in data.items():
            if isinstance(kk, str) and kk.startswith("UNKNOWN_COL"):
                try:
                    idx = int(kk.replace("UNKNOWN_COL", ""))
                    if idx == pk_idx:
                        return self.converter.convert_value(vv)
                except Exception:
                    continue
        return None

    def refresh_table_map_if_needed(
        self,
        table_map: Dict[str, str],
        collection_suffix: str,
        auto_mode: bool,
        auto_discover_new_tables: bool,
        auto_discover_interval_sec: int,
        last_refresh_ts_holder: Dict[str, float],
        reason: str = "",
    ):
        """
        last_refresh_ts_holder: {"ts": float} 作为可变引用，避免 worker 里堆字段
        """
        if (not auto_mode) or (not auto_discover_new_tables):
            return

        now = time.time()
        interval = max(1, int(auto_discover_interval_sec or 10))
        if now - last_refresh_ts_holder.get("ts", 0.0) < interval:
            return

        try:
            tables = self.list_tables()
            added = 0
            for t in tables:
                if t not in table_map:
                    table_map[t] = t + collection_suffix
                    added += 1
                    self._table_columns_cache.pop(t, None)
                    self._table_columns_cache_ts.pop(t, None)
                    self._pk_index_cache.pop(t, None)
            last_refresh_ts_holder["ts"] = now
            if added > 0:
                log(self.task_id, f"Discovered new tables={added} reason={reason}")
        except Exception as e:
            log(self.task_id, f"Refresh table_map failed: {str(e)[:180]}")
