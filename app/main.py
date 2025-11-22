from fastapi import FastAPI, Depends, WebSocket
from .dashboard import router as dashboard_router
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from contextlib import asynccontextmanager
from .deps import get_duck
from .settings import get_settings
import time

app = FastAPI(title="Quant Platform Minimal")
app.include_router(dashboard_router)

@app.get("/metrics")
def metrics():
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/api/health")
def health():
    # simple query - open and close connection per request
    with get_duck(read_only=True) as duck:
        duck.execute("SELECT 1").fetchone()
    return {"status": "ok"}

@app.get("/api/trusted/bars")
def get_bars(symbol: str, start: str, end: str):
    q = """
SELECT std_symbol, bar_time, open, high, low, close, volume, vwap, trades, file_date
FROM fact_bars_1m_trusted
WHERE std_symbol = ? AND bar_time BETWEEN CAST(? AS TIMESTAMP) AND CAST(? AS TIMESTAMP)
ORDER BY bar_time"""
    with get_duck(read_only=True) as duck:
        rows = duck.execute(q, [symbol, start, end]).fetchall()
        # Convert tuples to dictionaries for proper JSON response
        return [
            {
                "std_symbol": row[0],
                "bar_time": str(row[1]),
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "volume": row[6],
                "vwap": row[7],
                "trades": row[8],
                "file_date": str(row[9]) if row[9] else None
            }
            for row in rows
        ]

@app.get("/api/validation/results")
def validation_results(date: str):
    q = """SELECT run_id, std_symbol, file_date, rule_name, metric, threshold, passed, details
           FROM validation_results WHERE file_date = ?"""
    try:
        with get_duck(read_only=True) as duck:
            rows = duck.execute(q, [date]).fetchall()
            # Convert tuples to dictionaries for proper JSON response
            return [
                {
                    "run_id": str(row[0]),
                    "std_symbol": row[1],
                    "file_date": str(row[2]),
                    "rule_name": row[3],
                    "metric": row[4],
                    "threshold": row[5],
                    "passed": bool(row[6]),
                    "details": row[7]
                }
                for row in rows
            ]
    except Exception:
        return []

@app.websocket("/ws/quotes")
async def ws_quotes(ws: WebSocket):
    await ws.accept()
    await ws.send_json({"hello": "quotes"})
    # demo: send heartbeat
    for _ in range(3):
        await ws.send_json({"ts": time.time(), "msg": "heartbeat"})
        await ws.receive_text()
    await ws.close()
# --- Web dashboard & status endpoints injected by apply_patch_v3.py ---

import os as _os_for_dash
import redis as _redis_for_dash
from fastapi.responses import HTMLResponse, JSONResponse  # type: ignore

_redis_url_for_dash = _os_for_dash.getenv("REDIS_URL", "redis://localhost:6379/0")
_r_dash = _redis_for_dash.Redis.from_url(_redis_url_for_dash, decode_responses=True)


@app.get("/api/status")
def api_status():
    stat = _r_dash.hgetall("sub:stat") or {}
    pinned = list(_r_dash.smembers("sub:pinned:set") or [])
    focus = list(_r_dash.lrange("sub:focus:queue", 0, -1) or [])
    current = list(_r_dash.smembers("sub:focus:current") or [])
    return JSONResponse(
        {
            "sub_stat": stat,
            "pinned": pinned,
            "focus_queue": focus,
            "focus_current": current,
        }
    )


@app.get("/sub-dashboard", response_class=HTMLResponse)
def sub_dashboard():
    return """<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Quant Platform Dashboard (Legacy)</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; }
    pre { background: #111; color: #0f0; padding: 10px; border-radius: 4px; max-height: 400px; overflow: auto; }
  </style>
</head>
<body>
  <h1>Quant Platform Dashboard (Legacy)</h1>
  <button onclick="reload()">Reload</button>
  <pre id="out">Loading...</pre>
  <script>
    async function reload() {
      const res = await fetch('/api/status');
      const data = await res.json();
      document.getElementById('out').textContent = JSON.stringify(data, null, 2);
    }
    reload();
  </script>
</body>
</html>
"""
