import os
import base64
import json
import time
from typing import Dict, Any

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except Exception:
    AESGCM = None

_KEY_DIR = "configs_keys"
os.makedirs(_KEY_DIR, exist_ok=True)


def _read_task_key(task_id: str) -> bytes:
    p = os.path.join(_KEY_DIR, f"{task_id}.key")
    if os.path.exists(p):
        with open(p, "rb") as f:
            b64 = f.read().decode("ascii")
            kb = base64.b64decode(b64)
            if len(kb) in (16, 24, 32):
                return kb
    return b""


def _write_task_key(task_id: str, kb: bytes):
    p = os.path.join(_KEY_DIR, f"{task_id}.key")
    with open(p, "wb") as f:
        f.write(base64.b64encode(kb))


def get_or_create_task_key(task_id: str) -> bytes:
    if AESGCM is None:
        raise RuntimeError("cryptography not installed; cannot encrypt configs")
    kb = _read_task_key(task_id)
    if kb:
        return kb
    kb = os.urandom(32)
    _write_task_key(task_id, kb)
    return kb


def encrypt_config_for_task(task_id: str, data: Dict[str, Any]) -> str:
    key = get_or_create_task_key(task_id)
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    aad = b"sync-config"
    pt = json.dumps(data, ensure_ascii=False).encode("utf-8")
    ct = aesgcm.encrypt(nonce, pt, aad)
    doc = {
        "enc": True,
        "alg": "AES-GCM",
        "ts": int(time.time()),
        "nonce_b64": base64.b64encode(nonce).decode("ascii"),
        "ct_b64": base64.b64encode(ct).decode("ascii"),
    }
    return json.dumps(doc, ensure_ascii=False)


def decrypt_config_for_task(task_id: str, text: str) -> Dict[str, Any]:
    obj = json.loads(text)
    if isinstance(obj, dict) and obj.get("enc") is True and obj.get("alg") == "AES-GCM":
        key = get_or_create_task_key(task_id)
        aesgcm = AESGCM(key)
        nonce = base64.b64decode(obj["nonce_b64"])
        ct = base64.b64decode(obj["ct_b64"])
        aad = b"sync-config"
        pt = aesgcm.decrypt(nonce, ct, aad)
        return json.loads(pt.decode("utf-8"))
    if isinstance(obj, dict):
        return obj
    raise RuntimeError("Invalid config content")
