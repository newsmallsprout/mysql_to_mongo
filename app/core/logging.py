# app/core/logging.py
def log(task_id: str, msg: str):
    print(f"[{task_id}] {msg}", flush=True)
