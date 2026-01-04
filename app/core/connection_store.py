import os
import json
import glob
from typing import List, Optional, Dict, Any
from app.core.secret_store import encrypt_config_for_task, decrypt_config_for_task

_CONN_DIR = "connections"

class ConnectionStore:
    def __init__(self):
        os.makedirs(_CONN_DIR, exist_ok=True)

    def _get_path(self, conn_id: str) -> str:
        return os.path.join(_CONN_DIR, f"{conn_id}.json")

    def save(self, conn_id: str, config: Dict[str, Any]):
        """
        Save connection config (encrypted).
        """
        # Ensure conn_id is safe
        conn_id = conn_id.strip()
        if not conn_id or "/" in conn_id or "\\" in conn_id:
            raise ValueError("Invalid connection ID")

        path = self._get_path(conn_id)
        encrypted_str = encrypt_config_for_task(conn_id, config)
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(encrypted_str)

    def load(self, conn_id: str) -> Optional[Dict[str, Any]]:
        """
        Load connection config (decrypted).
        """
        path = self._get_path(conn_id)
        if not os.path.exists(path):
            return None
        
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
            return decrypt_config_for_task(conn_id, content)

    def list_all(self) -> List[Dict[str, Any]]:
        """
        List all connections (summary info only).
        """
        results = []
        for p in glob.glob(os.path.join(_CONN_DIR, "*.json")):
            try:
                conn_id = os.path.splitext(os.path.basename(p))[0]
                config = self.load(conn_id)
                if config:
                    # Return safe summary
                    results.append({
                        "id": conn_id,
                        "type": config.get("type", "unknown"),
                        "name": config.get("name", conn_id),
                        "host": config.get("host"),
                        "port": config.get("port"),
                        "user": config.get("user"),
                        # Do not return password in list
                    })
            except Exception as e:
                print(f"Error loading connection {p}: {e}")
        return results

    def delete(self, conn_id: str):
        path = self._get_path(conn_id)
        if os.path.exists(path):
            os.remove(path)
        
        # Also try to remove the key file if it exists
        key_path = os.path.join("configs_keys", f"{conn_id}.key")
        if os.path.exists(key_path):
            os.remove(key_path)

connection_store = ConnectionStore()
