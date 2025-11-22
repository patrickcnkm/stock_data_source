
# workers/sub_rotation.py
import time, argparse, random
from app.deps import get_redis
from app.settings import get_settings
from app.stream_consts import TICKS_PREFIX

PINNED_KEY = "sub:pinned:set"
FOCUS_Q = "sub:focus:queue"
ORDERBOOK_HK = "sub:orderbook:hk:set"
SUB_STAT = "sub:stat"
LAST_SWITCH = "sub:last_switch_at"

def setup_focus_queue(r, candidates):
    # Simple FIFO queue implemented via Redis List
    r.delete(FOCUS_Q)
    for s in candidates:
        r.rpush(FOCUS_Q, s)

def next_focus_batch(r, batch_size):
    batch = []
    for _ in range(batch_size):
        s = r.lpop(FOCUS_Q)
        if not s:
            break
        batch.append(s)
    # rotate back
    for s in batch:
        r.rpush(FOCUS_Q, s)
    return batch

def mock_query_subscription(r):
    # In real mode, call futu query_subscription(); here store mock stats
    used = int(r.hget(SUB_STAT, "used") or 0)
    remain = int(r.hget(SUB_STAT, "remain") or 300)
    return used, remain

def ensure_pinned(r, pinned):
    r.sadd(PINNED_KEY, *pinned)

def run_rotation_demo(pinned, candidates, batch_size=40, hold_sec=75):
    r = get_redis()
    ensure_pinned(r, pinned)
    setup_focus_queue(r, [c for c in candidates if c not in pinned])

    while True:
        used, remain = mock_query_subscription(r)
        batch = next_focus_batch(r, batch_size)
        # mock subscribe: record current focus set
        r.delete("sub:focus:current")
        if batch:
            r.sadd("sub:focus:current", *batch)
        r.set(LAST_SWITCH, int(time.time()))
        print(f"[ROTATE] focus -> {batch[:5]}... total {len(batch)} (hold {hold_sec}s)")
        time.sleep(hold_sec)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pinned", nargs="+", default=["00700.HK","AAPL"])
    ap.add_argument("--candidates", nargs="+", default=["MSFT","GOOG","NVDA","META","BABA","3690.HK","0700.HK","TSLA","AMD","9988.HK"])
    ap.add_argument("--batch-size", type=int, default=40)
    ap.add_argument("--hold-sec", type=int, default=75)
    ap.add_argument("--demo", action="store_true")
    args = ap.parse_args()

    if args.demo:
        run_rotation_demo(args.pinned, args.candidates, args.batch_size, args.hold_sec)
    else:
        # TODO: implement real futu subscribe/unsubscribe per batch respecting 300 cap and HK orderbook<50
        run_rotation_demo(args.pinned, args.candidates, args.batch_size, args.hold_sec)

if __name__ == "__main__":
    main()
