# app/sync/mongo_writer.py
import time
import random
from collections import Counter
from typing import List

from pymongo.errors import BulkWriteError, AutoReconnect, OperationFailure
from app.core.logging import log


class MongoWriter:
    def __init__(self, task_id: str, stop_event):
        self.task_id = task_id
        self.stop_event = stop_event

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
            self.task_id,
            f"BulkWriteError t={table} c={coll_name} errors={len(write_errors)} codes={dict(code_counter)} samples={samples}",
        )
        return write_errors

    def safe_bulk_write(self, coll, ops: List, table: str, coll_name: str, max_retry: int = 6) -> bool:
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
                only_dup = (len(write_errors) > 0) and all(w.get("code") == 11000 for w in write_errors)
                if only_dup:
                    return True
                has_215 = any(w.get("code") == 215 for w in write_errors)
                if not has_215:
                    return False
            except (AutoReconnect, OperationFailure) as e:
                log(self.task_id, f"Mongo transient error: {str(e)[:180]}")

            time.sleep(min(30.0, backoff) + random.random() * 0.2)
            backoff *= 2

        log(self.task_id, f"Mongo write failed after retries t={table} c={coll_name} batch={len(ops)}")
        return False
