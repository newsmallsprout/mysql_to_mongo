# app/core/config_store.py
import os
import json
from app.api.models import SyncTaskRequest

TASK_CONFIG_DIR = "configs"
os.makedirs(TASK_CONFIG_DIR, exist_ok=True)


def save_task_config(config: SyncTaskRequest):
    p = os.path.join(TASK_CONFIG_DIR, f"{config.task_id}.json")
    with open(p, "w", encoding="utf-8") as f:
        f.write(config.model_dump_json())


def delete_task_config(task_id: str):
    p = os.path.join(TASK_CONFIG_DIR, f"{task_id}.json")
    if os.path.exists(p):
        os.remove(p)


def iter_task_config_files():
    for name in os.listdir(TASK_CONFIG_DIR):
        if name.endswith(".json"):
            yield os.path.join(TASK_CONFIG_DIR, name)


def load_task_config_file(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)
