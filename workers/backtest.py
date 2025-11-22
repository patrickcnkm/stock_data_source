# workers/backtest.py
"""极简回测引擎：使用 DuckDB 中的可信 tick/bars 数据，重放 VWAP Break 策略。

依赖表结构（请根据实际情况调整）：
    fact_ticks_trusted(symbol, ts_ex, price, ...)

使用方式：
    python -m workers.backtest --symbol 00700.HK --start 2025-11-19 --end 2025-11-21
"""

from __future__ import annotations
import argparse
from dataclasses import dataclass
from datetime import datetime
from typing import List

import duckdb
import pandas as pd

from app.settings import get_settings

settings = get_settings()


@dataclass
class Trade:
    ts: datetime
    side: str
    price: float


class VWAPBacktester:
    def __init__(self, window: int = 20, eps: float = 0.001):
        self.window = window
        self.eps = eps
        self.prices: List[float] = []
        self.position = 0
        self.trades: List[Trade] = []

    def on_tick(self, ts: datetime, price: float):
        self.prices.append(price)
        if len(self.prices) > self.window:
            self.prices.pop(0)
        if len(self.prices) < self.window:
            return

        avg = sum(self.prices) / len(self.prices)
        up = avg * (1 + self.eps)
        down = avg * (1 - self.eps)

        if price > up and self.position <= 0:
            self.position = 1
            self.trades.append(Trade(ts=ts, side="BUY", price=price))
        elif price < down and self.position >= 0:
            self.position = -1
            self.trades.append(Trade(ts=ts, side="SELL", price=price))

    def summarize(self):
        if not self.trades:
            return {"trades": 0, "pnl": 0.0, "roundtrips": 0, "win_rate": 0.0}

        pnl = 0.0
        roundtrips = 0
        win = 0
        for i in range(1, len(self.trades)):
            a = self.trades[i - 1]
            b = self.trades[i]
            if a.side == b.side:
                continue
            roundtrips += 1
            if a.side == "BUY":
                p = b.price - a.price
            else:
                p = a.price - b.price
            pnl += p
            if p > 0:
                win += 1

        win_rate = win / roundtrips if roundtrips else 0.0
        return {
            "trades": len(self.trades),
            "roundtrips": roundtrips,
            "pnl": pnl,
            "win_rate": win_rate,
        }


def load_ticks(symbol: str, start: str, end: str) -> pd.DataFrame:
    conn = duckdb.connect(settings.duckdb_path, read_only=True)
    try:
        df = conn.execute(
            """
            SELECT ts_ex, price
            FROM fact_ticks_trusted
            WHERE symbol = ?
              AND ts_ex >= ?
              AND ts_ex < ?
            ORDER BY ts_ex
            """,
            [symbol, start, end],
        ).fetch_df()
    finally:
        conn.close()
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--window", type=int, default=20)
    ap.add_argument("--eps", type=float, default=0.001)
    args = ap.parse_args()

    df = load_ticks(args.symbol, args.start, args.end)
    if df.empty:
        print("[backtest] no data")
        return

    bt = VWAPBacktester(window=args.window, eps=args.eps)
    for _, row in df.iterrows():
        bt.on_tick(row["ts_ex"], float(row["price"]))

    summary = bt.summarize()
    print(f"[backtest] {args.symbol} {args.start} ~ {args.end}")
    print(summary)


if __name__ == "__main__":
    main()
