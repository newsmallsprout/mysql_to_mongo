# app/sync/task_manager.py
import threading
from typing import Dict, List

from app.api.models import SyncTaskRequest
from app.core.config_store import save_task_config, delete_task_config, iter_task_config_files, load_task_config_file
from app.core.logging import log
from app.sync.worker import SyncWorker


class TaskManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._tasks: Dict[str, SyncWorker] = {}

    def start(self, cfg: SyncTaskRequest):
        save_task_config(cfg)
        w = SyncWorker(cfg)
        with self._lock:
            self._tasks[cfg.task_id] = w
        threading.Thread(target=w.run, daemon=True).start()

    def stop(self, task_id: str):
        delete_task_config(task_id)
        with self._lock:
            w = self._tasks.get(task_id)
            if w is not None:
                w.stop()
                del self._tasks[task_id]

    def list_tasks(self) -> List[str]:
        with self._lock:
            return list(self._tasks.keys())

    def restore_from_disk(self):
        for p in iter_task_config_files():
            try:
                cfg_dict = load_task_config_file(p)
                cfg = SyncTaskRequest(**cfg_dict)
                self.start(cfg)
            except Exception as e:
                log("startup", f"restore failed {p}: {type(e).__name__}: {str(e)[:200]}")


task_manager = TaskManager()
