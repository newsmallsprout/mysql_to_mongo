# app/core/state_store.py
import os
import json
from typing import Optional, Dict, Any

STATE_DIR = "state"
os.makedirs(STATE_DIR, exist_ok=True)


def load_state(task_id: str) -> Optional[Dict[str, Any]]:
    p = os.path.join(STATE_DIR, f"{task_id}.json")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_state(task_id: str, log_file: str, log_pos: int):
    p = os.path.join(STATE_DIR, f"{task_id}.json")
    with open(p, "w", encoding="utf-8") as f:
        json.dump({"log_file": log_file, "log_pos": log_pos}, f)
