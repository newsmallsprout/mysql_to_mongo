# app/core/uri.py
from urllib.parse import quote_plus
from app.api.models import DBConfig


def build_mongo_uri(m: DBConfig) -> str:
    u = quote_plus(m.user)
    p = quote_plus(m.password)
    db = m.database or "sync_db"
    auth_source = quote_plus(m.auth_source or "admin")

    if m.hosts:
        hosts = ",".join(m.hosts)
        params = []
        if m.replica_set:
            rs = quote_plus(m.replica_set)
            params.append(f"replicaSet={rs}")
        params.append("retryWrites=true")
        params.append(f"authSource={auth_source}&authMechanism=SCRAM-SHA-256")
        qp = "&".join(params)
        return f"mongodb://{u}:{p}@{hosts}/{db}?{qp}"

    host = m.host or "localhost"
    port = int(m.port or 27017)
    return (
        f"mongodb://{u}:{p}@{host}:{port}/{db}"
        f"?retryWrites=true&authSource={auth_source}&authMechanism=SCRAM-SHA-256"
    )
