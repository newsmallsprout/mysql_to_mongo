# app/core/logging.py
import os
from datetime import datetime

def log(task_id: str, msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{task_id}] {msg}"
    print(line, flush=True)
    
    # Try to write to logs/{task_id}.log
    try:
        if not os.path.exists("logs"):
            os.makedirs("logs", exist_ok=True)
        with open(os.path.join("logs", f"{task_id}.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
