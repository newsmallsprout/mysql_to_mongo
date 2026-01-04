# app/sync/task_manager.py
import threading
from typing import Dict, List

from app.api.models import SyncTaskRequest
from app.core.config_store import save_task_config, delete_task_config, iter_task_config_files, load_task_config_file
from app.core.logging import log
from app.sync.worker import SyncWorker
import os


class TaskManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._tasks: Dict[str, SyncWorker] = {}

    def is_running(self, task_id: str) -> bool:
        with self._lock:
            w = self._tasks.get(task_id)
            return bool(w and getattr(w, "_status", None) == "running")

    def start(self, cfg: SyncTaskRequest):
        save_task_config(cfg)
        w = SyncWorker(cfg)
        with self._lock:
            self._tasks[cfg.task_id] = w
        threading.Thread(target=w.run, daemon=True).start()

    def start_by_id(self, task_id: str):
        # 加载已保存的配置并启动（从记录点位继续）
        cfg_dict = load_task_config_file(os.path.join("configs", f"{task_id}.json"))
        cfg = SyncTaskRequest(**cfg_dict)
        w = SyncWorker(cfg)
        with self._lock:
            self._tasks[task_id] = w
        threading.Thread(target=w.run, daemon=True).start()

    def stop(self, task_id: str):
        with self._lock:
            w = self._tasks.get(task_id)
            if w is not None:
                w.stop()
                del self._tasks[task_id]
        log(task_id, "Task stopped")

    def stop_soft(self, task_id: str):
        with self._lock:
            w = self._tasks.get(task_id)
            if w is not None:
                w.stop()
                try:
                    w._status = "stopped"
                except Exception:
                    pass
        log(task_id, "Task stopped (soft)")

    def delete(self, task_id: str):
        # stop if running
        self.stop_soft(task_id)
        # delete config
        delete_task_config(task_id)
        # delete state
        sp = os.path.join("state", f"{task_id}.json")
        if os.path.exists(sp):
            os.remove(sp)
        # delete logs
        lp = os.path.join("logs", f"{task_id}.log")
        if os.path.exists(lp):
            os.remove(lp)
        log(task_id, "Task deleted")

    def reset(self, task_id: str):
        # remove state only
        sp = os.path.join("state", f"{task_id}.json")
        if os.path.exists(sp):
            os.remove(sp)
        log(task_id, "Task reset (state cleared)")

    def list_tasks(self) -> List[str]:
        with self._lock:
            return list(self._tasks.keys())

    def get_all_tasks_status(self) -> List[Dict]:
        res = []
        with self._lock:
            for tid, w in self._tasks.items():
                res.append(w.get_status())
        return res

    def restore_from_disk(self):
        for p in iter_task_config_files():
            try:
                cfg_dict = load_task_config_file(p)
                cfg = SyncTaskRequest(**cfg_dict)
                self.start(cfg)
            except Exception as e:
                log("startup", f"restore failed {p}: {type(e).__name__}: {str(e)[:200]}")


task_manager = TaskManager()
