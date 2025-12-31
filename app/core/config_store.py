# app/core/config_store.py
import os
import json
from app.api.models import SyncTaskRequest
from app.core.secret_store import encrypt_config_for_task, decrypt_config_for_task

TASK_CONFIG_DIR = "configs"
os.makedirs(TASK_CONFIG_DIR, exist_ok=True)


def save_task_config(config: SyncTaskRequest):
    p = os.path.join(TASK_CONFIG_DIR, f"{config.task_id}.json")
    payload = encrypt_config_for_task(config.task_id, config.model_dump())
    with open(p, "w", encoding="utf-8") as f:
        f.write(payload)


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
        text = f.read()
        name = os.path.basename(path)
        task_id = name[:-5] if name.endswith(".json") else name
        return decrypt_config_for_task(task_id, text)
