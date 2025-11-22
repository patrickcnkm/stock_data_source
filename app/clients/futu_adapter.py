
# app/clients/futu_adapter.py
# Optional adapter for Futu OpenD. Works only if 'futu' package and OpenD are available.
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional
import datetime as dt

try:
    from futu import OpenQuoteContext, RET_OK, SubType, KLType  # type: ignore
    FUTU_AVAILABLE = True
except Exception:
    OpenQuoteContext = object   # type: ignore
    RET_OK = 0
    class SubType:  # placeholders
        TICKER = "TICKER"
        QUOTE = "QUOTE"
    class KLType:
        K_1M = "K_1M"
    FUTU_AVAILABLE = False

@dataclass
class FutuSettings:
    ip: str
    quote_port: int

class FutuAdapter:
    def __init__(self, ip: str, quote_port: int):
        if not FUTU_AVAILABLE:
            raise RuntimeError("Futu SDK not available. Please `pip install futu-api` and run OpenD.")
        self.ctx = OpenQuoteContext(host=ip, port=quote_port)

    def fetch_ticks_day(self, symbol: str, trade_date: dt.date):
        """Fetch TICKER (tick trades) for a given date.
        NOTE: The exact API varies by SDK version. Common patterns involve:
          - self.ctx.get_history_kl(...) for bars
          - self.ctx.request_history_kline(...) for historical bars
          - For ticks, use self.ctx.request_trades(...) or similar if available,
            or fallback to minute bars then reconstruct.
        This method should be updated to your installed SDK methods.
        """
        # Placeholder: attempt to fetch 1-minute bars if tick endpoint not available.
        ret, data = self.ctx.get_history_kl(symbol, start=trade_date.isoformat(),
                                            end=(trade_date+dt.timedelta(days=1)).isoformat(),
                                            ktype=KLType.K_1M)
        if ret != RET_OK:
            raise RuntimeError(f"Futu get_history_kl failed: {data}")
        # Convert bars to pseudo-ticks (one per bar close) as a fallback demo
        rows = []
        for _, row in data.iterrows():
            rows.append({
                "ts_exchange": row["time"],
                "price": float(row["close"]),
                "size": int(row.get("volume", 0)),
                "trade_id": f"{symbol}-{row['time']}",
                "cond": "BARCLOSE"
            })
        return rows

    def close(self):
        if FUTU_AVAILABLE and hasattr(self, "ctx"):
            self.ctx.close()
