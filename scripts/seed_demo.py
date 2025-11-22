import os, pandas as pd, numpy as np
from datetime import datetime, timedelta, date
import random, pathlib

random.seed(42)
np.random.seed(42)

symbols = ['00700.HK', 'AAPL']

def gen_day(symbol, d: date):
    # generate 3000 ticks over regular session (mock)
    base_price = 300 if symbol.endswith('.HK') else 180
    ts = []
    px = []
    sz = []
    trade_id = []
    cond = []
    start = datetime(d.year, d.month, d.day, 9, 30)  # mock
    for i in range(3000):
        t = start + timedelta(seconds=i*5)
        ts.append(t.isoformat())
        base_price *= (1 + np.random.normal(0, 0.0002))
        px.append(round(base_price, 3))
        sz.append(int(abs(np.random.normal(200, 50))))
        trade_id.append(f"{symbol}-{d.isoformat()}-{i}")
        cond.append('N')
    return pd.DataFrame({
        'ts_exchange': ts, 'price': px, 'size': sz, 'trade_id': trade_id, 'cond': cond
    })

def main():
    end = date.today()
    days = [end - timedelta(days=i+1) for i in range(2)]
    for s in symbols:
        for d in days:
            p = pathlib.Path(f"raw_demo/{s}")
            p.mkdir(parents=True, exist_ok=True)
            df = gen_day(s, d)
            df.to_csv(p / f"{d.isoformat()}.csv", index=False)
            print("seeded", s, d)

if __name__ == "__main__":
    main()