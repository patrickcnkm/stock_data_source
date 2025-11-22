# app/deps.py
import duckdb, os
from contextlib import contextmanager
from .settings import get_settings

@contextmanager
def get_duck(read_only: bool = True):
    """
    Get a DuckDB connection as a context manager.
    Opens and closes connections per request to avoid conflicts with workers.
    Defaults to read-only for API endpoints.
    """
    settings = get_settings()
    db_path = settings.duckdb_path
    # auto-create parent folder if needed (only for write mode)
    if not read_only:
        parent = os.path.dirname(db_path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
    # open database file
    con = duckdb.connect(db_path, read_only=read_only)
    try:
        yield con
    finally:
        con.close()

def get_redis():
    # import lazily to avoid errors when Redis isn't running
    import redis
    from .settings import get_settings
    return redis.Redis.from_url(get_settings().redis_url)