# app/core/uri.py
from urllib.parse import quote_plus
from app.api.models import DBConfig


def build_mongo_uri(m: DBConfig) -> str:
    u = quote_plus(m.user)
    p = quote_plus(m.password)
    db = m.database or "sync_db"
    auth_source = quote_plus(m.auth_source or "admin")

    if m.hosts and m.replica_set:
        hosts = ",".join(m.hosts)
        rs = quote_plus(m.replica_set)
        return (
            f"mongodb://{u}:{p}@{hosts}/{db}"
            f"?replicaSet={rs}&retryWrites=true"
            f"&authSource={auth_source}&authMechanism=SCRAM-SHA-256"
        )

    host = m.host or "localhost"
    port = int(m.port or 27017)
    return (
        f"mongodb://{u}:{p}@{host}:{port}/{db}"
        f"?retryWrites=true&authSource={auth_source}&authMechanism=SCRAM-SHA-256"
    )
