
# workers/streams_gateway.py
import argparse, time, duckdb, pandas as pd, redis
from datetime import date, timedelta
from app.settings import get_settings
from app.stream_consts import TICKS_PREFIX

def publish_ticks_from_staging(r: redis.Redis, con, symbol: str, file_date: str, maxlen=10000, sleep_ms=2):
    q = "
    SELECT ts_exchange, price, size FROM fact_ticks_staging
    WHERE std_symbol=? AND file_date=? ORDER BY ts_exchange
    "
    df = con.execute(q, [symbol, file_date]).df()
    stream = f"{TICKS_PREFIX}{symbol}"
    for _, row in df.iterrows():
        r.xadd(stream, {
            "ts_ex": str(row["ts_exchange"]),
            "price": str(row["price"]),
            "size": str(int(row["size"]))
        }, maxlen=maxlen, approximate=True)
        if sleep_ms > 0:
            time.sleep(sleep_ms/1000.0)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", required=True)
    ap.add_argument("--days", type=int, default=2)
    ap.add_argument("--sleep-ms", type=int, default=2, help="per tick pacing to simulate realtime")
    args = ap.parse_args()

    # Create connections - read-only for DB since we only read, separate Redis connection
    settings = get_settings()
    con = duckdb.connect(settings.duckdb_path, read_only=True)
    import redis
    r = redis.Redis.from_url(settings.redis_url)
    end = date.today()
    dates = [ (end - timedelta(days=i+1)).isoformat() for i in range(args.days) ]

    for s in args.symbols:
        for d in dates:
            print(f"[STREAMS] publish {s} {d}")
            publish_ticks_from_staging(r, con, s, d, sleep_ms=args.sleep_ms)
    print("[STREAMS] done")

if __name__ == "__main__":
    main()
