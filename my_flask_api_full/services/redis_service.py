# services/redis_service.py
import os
import json
from typing import Any, Dict, Iterable, Optional, Tuple
import redis

# Prefer REDIS_URL if provided (works great in Docker), else fall back to host/port/db.
REDIS_URL = os.getenv("REDIS_URL")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB   = int(os.getenv("REDIS_DB", "0"))

common_kwargs = dict(
    socket_timeout=3,
    socket_connect_timeout=3,
    retry_on_timeout=True,
    health_check_interval=30,
    decode_responses=True,  # return str not bytes
)

if REDIS_URL:
    rdb = redis.from_url(REDIS_URL, **common_kwargs)
else:
    rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, **common_kwargs)

NAMESPACE = os.getenv("REDIS_NAMESPACE", "plan")  # e.g., plan:p-123

def make_key(object_id: str) -> str:
    return f"{NAMESPACE}:{object_id}"

def set_data(key: str, value: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
    payload = json.dumps(value, separators=(",", ":"), sort_keys=True)
    if ttl_seconds:
        rdb.setex(key, ttl_seconds, payload)
    else:
        rdb.set(key, payload)

def get_data(key: str) -> Optional[Dict[str, Any]]:
    raw = rdb.get(key)
    return json.loads(raw) if raw else None

def delete_data(key: str) -> bool:
    return rdb.delete(key) > 0

def exists(key: str) -> bool:
    return rdb.exists(key) == 1

def mget(keys: Iterable[str]) -> Dict[str, Optional[Dict[str, Any]]]:
    keys = list(keys)
    vals = rdb.mget(keys)
    return {k: (json.loads(v) if v else None) for k, v in zip(keys, vals)}

def scan_prefix(prefix: str, count: int = 1000):
    """Efficiently iterate keys instead of KEYS * (which blocks)."""
    cursor = 0
    pattern = f"{prefix}*"
    while True:
        cursor, keys = rdb.scan(cursor=cursor, match=pattern, count=count)
        for k in keys:
            yield k
        if cursor == 0:
            break

def health() -> Tuple[bool, Optional[str]]:
    try:
        rdb.ping()
        return True, None
    except Exception as e:
        return False, str(e)

# --- Convenience wrappers so controllers don't build keys manually ---

def set_by_id(object_id: str, value: Dict[str, Any], ttl_seconds: Optional[int] = None) -> None:
    set_data(make_key(object_id), value, ttl_seconds)

def get_by_id(object_id: str) -> Optional[Dict[str, Any]]:
    return get_data(make_key(object_id))

def delete_by_id(object_id: str) -> bool:
    return delete_data(make_key(object_id))
