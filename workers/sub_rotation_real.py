# workers/sub_rotation_real.py
# Real subscription rotation for Futu OpenD with debouncing (>=60s),
# exponential backoff, HK ORDER_BOOK cap, and Prometheus metrics.

import os, time, random, argparse
from typing import List, Tuple
from prometheus_client import Gauge, Counter, Histogram, start_http_server
from dotenv import load_dotenv
from app import alerts
load_dotenv()

FUTU_IP = os.getenv("FUTU_OPEND_IP", "127.0.0.1")
FUTU_QPORT = int(os.getenv("FUTU_OPEND_QUOTE_PORT", "11111"))
SUB_TOTAL = int(os.getenv("FUTU_SUB_TOTAL", "300"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

import redis
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

SUB_USED = Gauge("futu_sub_used", "Futu subscription used quota")
SUB_REMAIN = Gauge("futu_sub_remain", "Futu subscription remaining quota")
SUB_ROTATE_SUCCESS = Counter("futu_sub_rotate_success_total", "Rotation success count")
SUB_ROTATE_FAIL = Counter("futu_sub_rotate_fail_total", "Rotation failure count", ["phase"])
SUB_ORDERBOOK_HK = Gauge("futu_sub_orderbook_hk", "Current HK orderbook concurrent subscriptions")
SUB_HOLD_SECONDS = Gauge("futu_sub_hold_seconds", "Batch hold seconds")
SUB_LAST_SWITCH_TS = Gauge("futu_sub_last_switch_timestamp", "Unix timestamp of last switch")
SUB_API_LAT_MS = Histogram("futu_api_latency_ms", "Futu API call latency (ms)")

PINNED_KEY = "sub:pinned:set"
FOCUS_Q = "sub:focus:queue"
FOCUS_CUR = "sub:focus:current"
SUB_STAT = "sub:stat"
LAST_SWITCH = "sub:last_switch_at"

def now_ts(): return int(time.time())
def is_hk(s: str): return s.endswith(".HK") or s.endswith(".hk")

def _timed(call, *a, **k):
    t0 = time.time(); ret = call(*a, **k); t1 = time.time()
    SUB_API_LAT_MS.observe((t1 - t0) * 1000.0); return ret

def query_quota(ctx) -> Tuple[int,int]:
    used = 0
    if hasattr(ctx, "query_subscription"):
        ret, info = _timed(ctx.query_subscription)
        if ret == 0:
            if isinstance(info, dict) and "sub_list" in info:
                for v in info["sub_list"].values():
                    used += len(v or [])
            else:
                try: used = len(info)
                except: used = 0
        else:
            raise RuntimeError(str(info))
    else:
        ret, info = _timed(ctx.get_subscription)
        if ret != 0: raise RuntimeError(str(info))
        used = len(info)
    remain = max(0, SUB_TOTAL - used)
    SUB_USED.set(used); SUB_REMAIN.set(remain)
    r.hset(SUB_STAT, mapping={"used": used, "remain": remain, "ts": now_ts()})
    return used, remain

def ensure_pinned(pinned: List[str]):
    if pinned: r.sadd(PINNED_KEY, *pinned)

def setup_focus_queue(cands: List[str]):
    r.delete(FOCUS_Q)
    for s in cands: r.rpush(FOCUS_Q, s)

def next_focus_batch(limit: int) -> List[str]:
    batch = []
    for _ in range(limit):
        s = r.lpop(FOCUS_Q); 
        if not s: break
        batch.append(s)
    for s in batch: r.rpush(FOCUS_Q, s)  # rotate back
    return batch

def apply_hk_ob_cap(batch: List[str], cap: int=48) -> List[str]:
    hk_syms = [s for s in batch if is_hk(s)]
    allow = set(hk_syms[:cap])
    SUB_ORDERBOOK_HK.set(len(allow))
    return [s for s in batch if (s in allow or not is_hk(s))]

def sleep_until(ts: int):
    while True:
        if now_ts() >= ts: return
        time.sleep(1)

def debounced(last_switch: int, hold_sec: int) -> bool:
    return (now_ts() - last_switch) < hold_sec

def safe_sub(ctx, symbols: List[str], subtypes: List, retry=3, backoff=1.0):
    for i in range(retry):
        try:
            ret, msg = _timed(ctx.subscribe, symbols, subtypes, subscribe_push=True)
            if ret == 0: return
            raise RuntimeError(str(msg))
        except Exception:
            SUB_ROTATE_FAIL.labels(phase="subscribe").inc()
            alerts.alert("Futu subscribe failed", "phase=subscribe")
            if i == retry - 1: raise
            time.sleep(backoff); backoff *= 2

def safe_unsub(ctx, symbols: List[str], subtypes: List, retry=3, backoff=1.0):
    for i in range(retry):
        try:
            ret, msg = _timed(ctx.unsubscribe, symbols, subtypes)
            if ret == 0: return
            raise RuntimeError(str(msg))
        except Exception:
            SUB_ROTATE_FAIL.labels(phase="unsubscribe").inc()
            alerts.alert("Futu unsubscribe failed", "phase=unsubscribe")
            if i == retry - 1: raise
            time.sleep(backoff); backoff *= 2

def rotate_once(ctx, pinned: List[str], batch: List[str], hold_sec: int, orderbook_cap_hk: int):
    from futu import SubType
    pinned_hk = [s for s in pinned if is_hk(s)]
    if pinned:   safe_sub(ctx, pinned, [SubType.QUOTE, SubType.TICKER])
    if pinned_hk: safe_sub(ctx, apply_hk_ob_cap(pinned_hk, orderbook_cap_hk), [SubType.ORDER_BOOK])

    prev_batch = list(r.smembers(FOCUS_CUR) or [])
    if batch:
        safe_sub(ctx, batch, [SubType.TICKER])
        hk_batch = [s for s in batch if is_hk(s)]
        if hk_batch:
            safe_sub(ctx, apply_hk_ob_cap(hk_batch, orderbook_cap_hk), [SubType.ORDER_BOOK])

    r.delete(FOCUS_CUR); 
    if batch: r.sadd(FOCUS_CUR, *batch)
    last = now_ts(); r.set(LAST_SWITCH, last)
    SUB_LAST_SWITCH_TS.set(last); SUB_HOLD_SECONDS.set(hold_sec)

    sleep_until(last + hold_sec)

    if prev_batch:
        try: safe_unsub(ctx, prev_batch, [SubType.TICKER])
        except: pass
        try:
            hk_prev = [s for s in prev_batch if is_hk(s)]
            if hk_prev: safe_unsub(ctx, hk_prev, [SubType.ORDER_BOOK])
        except: pass

    SUB_ROTATE_SUCCESS.inc()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pinned", nargs="+", default=["00700.HK","AAPL"])
    ap.add_argument("--candidates", nargs="+", default=["MSFT","GOOG","NVDA","META","BABA","3690.HK","0700.HK","TSLA","AMD","9988.HK"])
    ap.add_argument("--batch-size", type=int, default=40)
    ap.add_argument("--hold-sec", type=int, default=75)
    ap.add_argument("--metrics-port", type=int, default=9109)
    args = ap.parse_args()

    if args.hold_sec < 60:
        raise SystemExit("hold-sec must be >= 60.")

    start_http_server(args.metrics_port)

    ensure_pinned(args.pinned)
    cands = [c for c in args.candidates if c not in args.pinned]
    random.shuffle(cands); setup_focus_queue(cands)

    from futu import OpenQuoteContext
    ctx = OpenQuoteContext(host=FUTU_IP, port=FUTU_QPORT)

    backoffs = [60, 300, 1800]; idx = 0
    try:
        while True:
            last = int(r.get(LAST_SWITCH) or 0)
            if debounced(last, args.hold_sec):
                sleep_until(last + args.hold_sec)

            try:
                query_quota(ctx); idx = 0
            except Exception:
                SUB_ROTATE_FAIL.labels(phase="quota").inc()
                alerts.alert("Futu quota query failed", "phase=quota")
                time.sleep(backoffs[idx]); idx = min(idx+1, len(backoffs)-1); continue

            pinned_cost = len(args.pinned) * 2  # QUOTE + TICKER（ORDER_BOOK 单独控）
            budget = max(0, SUB_TOTAL - 10 - pinned_cost)
            batch = next_focus_batch(min(args.batch_size, budget))
            if not batch:
                time.sleep(args.hold_sec); continue

            try:
                rotate_once(ctx, args.pinned, batch, args.hold_sec, orderbook_cap_hk=48)
                idx = 0
            except Exception:
                SUB_ROTATE_FAIL.labels(phase="rotate").inc()
                alerts.alert("Futu rotation step failed", "phase=rotate")
                time.sleep(backoffs[idx]); idx = min(idx+1, len(backoffs)-1); continue
    finally:
        ctx.close()

if __name__ == "__main__":
    main()
