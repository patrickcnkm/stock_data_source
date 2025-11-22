# workers/strategy_runner.py
"""Strategy Runner – 基于 Redis Streams 的实时策略执行器。

设计：
- 对每个 symbol 使用一个 stream: bus:ticks:<symbol>
- 使用 consumer group（支持水平扩展）
- 简单 demo 策略：N 条移动平均突破
- 生成信号写入 bus:signals:<strategy>:<symbol>
"""

from __future__ import annotations
import os, json, time, argparse
from collections import deque
from typing import Deque, Dict, Any

import redis
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

TICKS_PREFIX = "bus:ticks:"
SIG_PREFIX = "bus:signals:"


STRAT_LAT_MS = Histogram(
    "strategy_handle_latency_ms", "Strategy handle latency (ms)",
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500)
)
STRAT_SIG_TOTAL = Counter(
    "strategy_signal_total", "Total strategy signals generated",
    ["strategy", "symbol", "side"]
)
STRAT_LAG = Gauge(
    "strategy_stream_lag", "Pending messages in consumer group",
    ["symbol"]
)


class VWAPBreakStrategy:
    """极简示范策略。"""
    def __init__(self, symbol: str, window: int = 20, eps: float = 0.001):
        self.symbol = symbol
        self.window = window
        self.eps = eps
        self.prices: Deque[float] = deque(maxlen=window)
        self.position = 0  # +1 多头，-1 空头，0 空仓

    def on_tick(self, tick: Dict[str, Any]) -> Dict[str, Any] | None:
        try:
            price = float(tick.get("price", 0.0))
        except Exception:
            return None
        if price <= 0:
            return None

        self.prices.append(price)
        if len(self.prices) < self.window:
            return None

        avg = sum(self.prices) / len(self.prices)
        up = avg * (1 + self.eps)
        down = avg * (1 - self.eps)

        side = None
        if price > up and self.position <= 0:
            side = "BUY"
            self.position = 1
        elif price < down and self.position >= 0:
            side = "SELL"
            self.position = -1

        if side is None:
            return None

        return {
            "symbol": self.symbol,
            "side": side,
            "price": price,
            "avg": avg,
            "ts_ex": tick.get("ts_ex"),
        }


def ensure_group(stream: str, group: str):
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            return
        raise


def run_strategy(symbol: str, group: str, consumer: str, window: int, eps: float):
    stream = f"{TICKS_PREFIX}{symbol}"
    sig_stream = f"{SIG_PREFIX}vwap_break:{symbol}"
    ensure_group(stream, group)
    strat = VWAPBreakStrategy(symbol, window=window, eps=eps)

    while True:
        t0 = time.time()
        resp = r.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=50,
            block=1000,
        )
        if not resp:
            continue

        for _, msgs in resp:
            for msg_id, fields in msgs:
                tick = {k: v for k, v in fields.items()}
                sig = strat.on_tick(tick)

                if sig:
                    STRAT_SIG_TOTAL.labels(
                        strategy="vwap_break",
                        symbol=symbol,
                        side=sig["side"]
                    ).inc()
                    r.xadd(
                        sig_stream,
                        {k: (v if isinstance(v, str) else json.dumps(v))
                         for k, v in sig.items()},
                        maxlen=1000,
                        approximate=True,
                    )

                r.xack(stream, group, msg_id)

        t1 = time.time()
        STRAT_LAT_MS.observe((t1 - t0) * 1000.0)

        try:
            info = r.xinfo_consumers(stream, group)
            pending = sum(c.get("pending", 0) for c in info)
            STRAT_LAG.labels(symbol=symbol).set(pending)
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--group", default="strat_vwap_break")
    ap.add_argument("--consumer", default="worker-1")
    ap.add_argument("--window", type=int, default=20)
    ap.add_argument("--eps", type=float, default=0.001)
    ap.add_argument("--metrics-port", type=int, default=9110)
    args = ap.parse_args()

    start_http_server(args.metrics_port)
    run_strategy(
        symbol=args.symbol,
        group=args.group,
        consumer=args.consumer,
        window=args.window,
        eps=args.eps,
    )


if __name__ == "__main__":
    main()
