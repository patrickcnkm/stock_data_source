from __future__ import annotations

import subprocess
from datetime import date, datetime, timedelta
from typing import List, Optional, Literal

import duckdb
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .deps import get_duck
from .settings import get_settings

router = APIRouter()

# Trusted table for coverage & delete
TICKS_TABLE = "fact_ticks_trusted"


# ============================
# Pydantic models
# ============================

class IngestRequest(BaseModel):
    """
    mode:
      - partial: specify symbols (HK only for now), optional date range
      - full_last_60d/full_range: reserved for future (HK universe)
    """
    mode: Literal["partial", "full_last_60d", "full_range"]
    symbols: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class DeleteRequest(BaseModel):
    symbols: Optional[List[str]] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None


# ============================
# Helpers
# ============================

def _normalize_symbol(symbol: str) -> str:
    """
    Normalize to Futu style: HK.00700, US.AAPL, etc.
    Very simple: if 'HK.' / 'US.' prefix exists, keep; if endswith .HK, convert; if digits, assume HK.
    """
    s = symbol.strip().upper()
    if "." in s and (s.startswith("HK.") or s.startswith("US.") or s.startswith("SZ.") or s.startswith("SH.")):
        return s
    if s.endswith(".HK"):
        code = s[:-3]
        return f"HK.{code}"
    if s.isdigit() or s.replace(".", "").isdigit():
        return f"HK.{s}"
    return f"US.{s}"


# ============================
# 1) Ingestion + Verification
# ============================

@router.post("/api/dashboard/ingest")
def dashboard_ingest(req: IngestRequest):
    """
    Orchestrate a full pipeline run:

      1) workers.ingest_futu:
           Futu → fact_ticks_raw
      2) workers.validate (A group):
           vertical checks on staging → validation_results
      3) workers.validate_cross (B group, demo mode):
           futu vs futu 1m comparison → validation_results
      4) workers.commit:
           staging → fact_ticks_trusted / fact_bars_1m_trusted

    Current limitations:
      - Only mode=partial is supported (explicit symbol list).
      - start_date / end_date are OPTIONAL:
          * If both provided: use them to derive --days (max 60).
          * If not provided: default to last 2 days.
    """

    # ---- 1. symbols ----
    if req.mode != "partial":
        raise HTTPException(
            status_code=400,
            detail="Only mode=partial is supported at the moment. "
                   "FULL_LAST_60D/FULL_RANGE need a HK symbol universe implementation.",
        )

    if not req.symbols:
        raise HTTPException(status_code=400, detail="symbols is required for partial mode")

    symbols = [_normalize_symbol(s) for s in req.symbols]

    # ---- 2. compute days based on date range (or default) ----
    today = date.today()
    yesterday = today - timedelta(days=1)

    if req.start_date and req.end_date:
        if req.end_date < req.start_date:
            raise HTTPException(status_code=400, detail="end_date must be >= start_date")
        if req.end_date > yesterday:
            raise HTTPException(status_code=400, detail="end_date must be <= yesterday")

        # We want to cover [start_date, end_date] ∩ [*, yesterday]
        effective_end = min(req.end_date, yesterday)
        effective_start = req.start_date
        days = (effective_end - effective_start).days + 1
    elif not req.start_date and not req.end_date:
        # default: last 2 days = [today-2, today-1]
        days = 2
        effective_end = yesterday
        effective_start = today - timedelta(days=days)
    else:
        raise HTTPException(
            status_code=400,
            detail="Either provide BOTH start_date and end_date, or leave BOTH empty.",
        )

    if days <= 0:
        raise HTTPException(status_code=400, detail="Computed days <= 0, please check your date range.")

    # safety cap to avoid huge backfill
    if days > 60:
        days = 60
        effective_start = today - timedelta(days=days)
        effective_end = yesterday

    plan = {
        "mode": req.mode,
        "symbols": symbols,
        "days": days,
        "effective_range": {
            "from": effective_start.isoformat(),
            "to": effective_end.isoformat(),
        },
    }

    steps_log = []

    def run_step(name: str, cmd: List[str]):
        try:
            proc = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
            )
            steps_log.append(
                {
                    "step": name,
                    "cmd": " ".join(cmd),
                    "returncode": proc.returncode,
                    "stdout": (proc.stdout or "")[-4000:],
                    "stderr": (proc.stderr or "")[-4000:],
                    "status": "ok",
                }
            )
        except subprocess.CalledProcessError as e:
            steps_log.append(
                {
                    "step": name,
                    "cmd": " ".join(cmd),
                    "returncode": e.returncode,
                    "stdout": (e.stdout or "")[-4000:],
                    "stderr": (e.stderr or "")[-4000:],
                    "status": "failed",
                }
            )
            raise HTTPException(
                status_code=500,
                detail={
                    "step": name,
                    "cmd": " ".join(cmd),
                    "returncode": e.returncode,
                    "stderr": e.stderr,
                },
            )

    # 1) ingest_futu
    run_step(
        "ingest_futu",
        ["python", "-m", "workers.ingest_futu", "--symbols", *symbols, "--days", str(days)],
    )

    # 2) validate A (vertical)
    run_step(
        "validate_A",
        ["python", "-m", "workers.validate", "--run-a", "--symbols", *symbols, "--days", str(days)],
    )

    # 3) validate B (horizontal, demo mode)
    run_step(
        "validate_B_demo",
        ["python", "-m", "workers.validate_cross", "--symbols", *symbols, "--days", str(days), "--demo"],
    )

    # 4) Check validation_results: any failures? if yes -> DO NOT commit
    settings = get_settings()
    con = duckdb.connect(settings.duckdb_path, read_only=True)
    try:
        # workers' --days logic: yesterday, yesterday-1, ...
        date_list = [(yesterday - timedelta(days=i)).isoformat() for i in range(days)]

        fail_df = con.execute(
            """
            SELECT std_symbol, file_date, rule_name, metric, threshold, passed
            FROM validation_results
            WHERE std_symbol IN ({syms})
              AND file_date IN ({dates})
              AND passed = FALSE
            ORDER BY std_symbol, file_date, rule_name
            """.format(
                syms=",".join(["?"] * len(symbols)),
                dates=",".join(["?"] * len(date_list)),
            ),
            symbols + date_list,
        ).fetch_df()
    finally:
        con.close()

    validation_summary = {
        "has_failure": not fail_df.empty,
        "failures": [],
    }

    if not fail_df.empty:
        for _, row in fail_df.iterrows():
            validation_summary["failures"].append(
                {
                    "symbol": row["std_symbol"],
                    "file_date": row["file_date"],
                    "rule_name": row["rule_name"],
                    "metric": row["metric"],
                    "threshold": row["threshold"],
                }
            )
        return {
            "status": "validation_failed",
            "plan": plan,
            "steps": steps_log,
            "validation": validation_summary,
        }

    # 5) No failures -> commit staging → trusted
    run_step(
        "commit",
        ["python", "-m", "workers.commit", "--symbols", *symbols, "--days", str(days)],
    )

    return {
        "status": "success",
        "plan": plan,
        "steps": steps_log,
        "validation": validation_summary,
    }


# ============================
# 2) Coverage endpoints
# ============================

@router.get("/api/dashboard/coverage/summary")
def coverage_summary(duck=Depends(get_duck)):
    """Return min/max date and total distinct symbols in trusted table."""
    try:
        df = duck.execute(
            f"""
            SELECT
              MIN(date_trunc('day', ts_exchange)) AS min_date,
              MAX(date_trunc('day', ts_exchange)) AS max_date,
              COUNT(DISTINCT std_symbol)       AS total_symbols
            FROM {TICKS_TABLE}
            """
        ).fetch_df()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    if df.empty or df["min_date"][0] is None:
        return {
            "has_data": False,
            "min_date": None,
            "max_date": None,
            "total_symbols": 0,
        }

    row = df.iloc[0]
    return {
        "has_data": True,
        "min_date": row["min_date"].date().isoformat(),
        "max_date": row["max_date"].date().isoformat(),
        "total_symbols": int(row["total_symbols"]),
    }


@router.get("/api/dashboard/coverage/daily")
def coverage_daily(duck=Depends(get_duck)):
    """
    Per-day coverage:
      - trade_date
      - symbol_count
      - coverage: 'full' or 'partial' relative to max symbol_count
    """
    try:
        df = duck.execute(
            f"""
            WITH daily AS (
              SELECT
                date_trunc('day', ts_exchange) AS trade_date,
                COUNT(DISTINCT std_symbol)   AS symbol_count
              FROM {TICKS_TABLE}
              GROUP BY 1
            ),
            max_sym AS (
              SELECT MAX(symbol_count) AS max_count FROM daily
            )
            SELECT
              daily.trade_date,
              daily.symbol_count,
              max_sym.max_count,
              CASE WHEN daily.symbol_count = max_sym.max_count
                   THEN 'full'
                   ELSE 'partial'
              END AS coverage
            FROM daily
            CROSS JOIN max_sym
            ORDER BY daily.trade_date
            """
        ).fetch_df()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    days = []
    for _, row in df.iterrows():
        days.append(
            {
                "trade_date": row["trade_date"].date().isoformat(),
                "symbol_count": int(row["symbol_count"]),
                "coverage": row["coverage"],
            }
        )
    return {"days": days}


@router.get("/api/dashboard/coverage/day")
def coverage_day_details(trade_date: date, duck=Depends(get_duck)):
    """Return symbol list for a specific day in trusted table."""
    try:
        df = duck.execute(
            f"""
            SELECT DISTINCT std_symbol
            FROM {TICKS_TABLE}
            WHERE date_trunc('day', ts_exchange) = ?
            ORDER BY std_symbol
            """,
            [trade_date],
        ).fetch_df()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return {
        "trade_date": trade_date.isoformat(),
        "symbol_count": len(df),
        "symbols": df["std_symbol"].tolist(),
    }


# ============================
# 3) Delete endpoint
# ============================

@router.post("/api/dashboard/delete")
def dashboard_delete(req: DeleteRequest, duck=Depends(get_duck)):
    """
    Delete from trusted table by:
      - optional symbol list
      - optional date range
    If neither provided, delete ALL rows (be careful).
    """
    conditions = []
    params: List[object] = []

    if req.symbols:
        syms = [_normalize_symbol(s) for s in req.symbols]
        placeholders = ",".join(["?"] * len(syms))
        conditions.append(f"std_symbol IN ({placeholders})")
        params.extend(syms)

    if req.start_date and req.end_date:
        if req.end_date < req.start_date:
            raise HTTPException(status_code=400, detail="end_date must be >= start_date")
        start_dt = datetime.combine(req.start_date, datetime.min.time())
        end_dt = datetime.combine(req.end_date + timedelta(days=1), datetime.min.time())
        conditions.append("ts_exchange >= ? AND ts_exchange < ?")
        params.extend([start_dt, end_dt])
    elif (req.start_date and not req.end_date) or (req.end_date and not req.start_date):
        raise HTTPException(
            status_code=400,
            detail="Either provide BOTH start_date and end_date, or leave BOTH empty.",
        )

    where_clause = " AND ".join(conditions) if conditions else "1=1"

    try:
        cnt_df = duck.execute(
            f"SELECT COUNT(*) AS cnt FROM {TICKS_TABLE} WHERE {where_clause}",
            params,
        ).fetch_df()
        to_delete = int(cnt_df["cnt"][0])

        duck.execute(
            f"DELETE FROM {TICKS_TABLE} WHERE {where_clause}",
            params,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    return {
        "deleted_rows": to_delete,
        "symbols": req.symbols,
        "start_date": req.start_date.isoformat() if req.start_date else None,
        "end_date": req.end_date.isoformat() if req.end_date else None,
    }


# ============================
# 4) HTML dashboard page
# ============================

@router.get("/dashboard", response_class=HTMLResponse)
def dashboard_page():
    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>HK Tick Data Dashboard</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; }
    h2 { margin-top: 24px; }
    fieldset { margin-bottom: 16px; }
    pre { background: #111; color: #0f0; padding: 10px; border-radius: 4px; max-height: 260px; overflow: auto; font-size: 12px; }
    .status-box { padding: 12px; border-radius: 4px; margin: 8px 0; }
    .status-success { background: #d1fae5; border: 1px solid #10b981; color: #065f46; }
    .status-failed { background: #fee2e2; border: 1px solid #ef4444; color: #991b1b; }
    .status-partial { background: #fef3c7; border: 1px solid #f59e0b; color: #92400e; }
    .detail-toggle { background: #6b7280; color: white; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer; font-size: 11px; margin-left: 8px; }
    .detail-content { display: none; margin-top: 8px; padding: 8px; background: #f9fafb; border-radius: 4px; font-size: 11px; }
    .detail-content.show { display: block; }
    .step-item { margin: 4px 0; padding: 4px; }
    .step-success { color: #10b981; }
    .step-failed { color: #ef4444; }
    table { border-collapse: collapse; margin-top: 8px; }
    th, td { border: 1px solid #ccc; padding: 4px 8px; font-size: 13px; }
    th { background: #f0f0f0; }
    .partial { background: #fff4e5; }
    .full { background: #e8fff2; }
    .btn { padding: 4px 10px; cursor: pointer; }
    .btn-primary { background: #2563eb; color: white; border: none; border-radius: 4px; }
    .btn-danger { background: #dc2626; color: white; border: none; border-radius: 4px; }
    .btn-secondary { background: #6b7280; color: white; border: none; border-radius: 4px; }
    input, select, textarea { font-size: 13px; padding: 2px 4px; }
    label { font-size: 13px; }
    .flex-row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
  </style>
</head>
<body>
  <h1>HK Tick Data Dashboard</h1>

  <!-- 1. Ingestion + Verification -->
  <h2>1. Data Ingestion & Verification</h2>
  <fieldset>
    <legend>Ingestion Mode</legend>
    <div>
      <label>
        <input type="radio" name="mode" value="partial" checked>
        Partial: Specify stock(s) + date range
      </label><br>
      <label>
        <input type="radio" name="mode" value="full_last_60d" disabled>
        Full: All HK stocks, last 60 days (TODO: HK universe)
      </label><br>
      <label>
        <input type="radio" name="mode" value="full_range" disabled>
        Full: All HK stocks, custom date range (TODO: HK universe)
      </label>
    </div>

    <div class="flex-row" style="margin-top: 8px;">
      <label>Symbols (comma separated, HK only):</label>
      <input id="ingest-symbols" style="width: 280px;" placeholder="e.g. 00700.HK,00005.HK" />
    </div>
    <div class="flex-row" style="margin-top: 8px;">
      <label>Start date (optional):</label>
      <input type="date" id="ingest-start" />
      <label>End date (optional):</label>
      <input type="date" id="ingest-end" />
    </div>

    <div style="margin-top: 8px;">
      <button class="btn btn-primary" onclick="triggerIngest()">Run Ingestion + Verification</button>
    </div>
  </fieldset>
  <div id="ingest-result"></div>
  <div id="ingest-detail-wrapper" style="display: none;">
    <button class="detail-toggle" type="button" onclick="toggleDetail('ingest', event)">Show Details</button>
    <pre id="ingest-log" class="detail-content">[Ready]</pre>
  </div>

  <!-- 2. Coverage Overview -->
  <h2>2. Ingested Data Overview</h2>
  <button class="btn btn-secondary" onclick="loadCoverage()">Reload Coverage</button>
  <div id="coverage-summary" style="margin-top: 8px; font-size: 13px;"></div>
  <div id="coverage-table-wrapper" style="margin-top: 8px;"></div>

  <!-- 3. Data Operations (Delete) -->
  <h2>3. Data Operations (Delete)</h2>
  <fieldset>
    <legend>Delete from fact_ticks_trusted</legend>
    <div class="flex-row" style="margin-top: 8px;">
      <label>Symbols (optional, comma separated):</label>
      <input id="del-symbols" style="width: 280px;" placeholder="e.g. 00700.HK,00005.HK" />
    </div>
    <div class="flex-row" style="margin-top: 8px;">
      <label>Start date (optional):</label>
      <input type="date" id="del-start" />
      <label>End date (optional):</label>
      <input type="date" id="del-end" />
    </div>
    <div style="margin-top: 8px;">
      <button class="btn btn-danger" onclick="runDelete()">Delete</button>
    </div>
  </fieldset>
  <pre id="delete-log">[No delete run]</pre>

<script>
async function triggerIngest() {
  const mode = document.querySelector('input[name="mode"]:checked').value;
  const symbolsRaw = document.getElementById('ingest-symbols').value.trim();
  const start = document.getElementById('ingest-start').value;
  const end = document.getElementById('ingest-end').value;

  const body = { mode: mode };

  if (mode === 'partial') {
    body.symbols = symbolsRaw ? symbolsRaw.split(',').map(s => s.trim()).filter(Boolean) : [];
    if (start && end) {
      body.start_date = start;
      body.end_date = end;
    } else {
      body.start_date = null;
      body.end_date = null;
    }
  }

  const log = document.getElementById('ingest-log');
  log.textContent = '[Running] Sending request...';

  try {
    const res = await fetch('/api/dashboard/ingest', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    const data = await res.json();
    log.textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    log.textContent = 'Error: ' + e;
  }
}

async function loadCoverage() {
  const summaryDiv = document.getElementById('coverage-summary');
  const tableDiv = document.getElementById('coverage-table-wrapper');
  summaryDiv.textContent = 'Loading...';
  tableDiv.innerHTML = '';

  try {
    const [resSum, resDaily] = await Promise.all([
      fetch('/api/dashboard/coverage/summary'),
      fetch('/api/dashboard/coverage/daily'),
    ]);
    const sum = await resSum.json();
    const daily = await resDaily.json();

    if (!sum.has_data) {
      summaryDiv.textContent = 'No data in fact_ticks_trusted yet.';
      return;
    }

    summaryDiv.innerHTML = `
      Date range: <b>${sum.min_date}</b> ~ <b>${sum.max_date}</b><br/>
      Total distinct symbols: <b>${sum.total_symbols}</b>
    `;

    const days = daily.days || [];
    if (!days.length) {
      tableDiv.textContent = 'No daily coverage rows.';
      return;
    }

    let html = '<table><thead><tr><th>Trade Date</th><th>Symbol Count</th><th>Coverage</th></tr></thead><tbody>';
    for (const d of days) {
      const cls = d.coverage === 'full' ? 'full' : 'partial';
      html += `<tr class="${cls}"><td>${d.trade_date}</td><td>${d.symbol_count}</td><td>${d.coverage}</td></tr>`;
    }
    html += '</tbody></table>';
    tableDiv.innerHTML = html;
  } catch (e) {
    summaryDiv.textContent = 'Error loading coverage: ' + e;
  }
}

async function runDelete() {
  const symbolsRaw = document.getElementById('del-symbols').value.trim();
  const start = document.getElementById('del-start').value;
  const end = document.getElementById('del-end').value;
  const log = document.getElementById('delete-log');

  const body = {};
  if (symbolsRaw) {
    body.symbols = symbolsRaw.split(',').map(s => s.trim()).filter(Boolean);
  }
  if (start && end) {
    body.start_date = start;
    body.end_date = end;
  }

  log.textContent = '[Running] Deleting...';

  try {
    const res = await fetch('/api/dashboard/delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    const data = await res.json();
    log.textContent = JSON.stringify(data, null, 2);
  } catch (e) {
    log.textContent = 'Error: ' + e;
  }
}
</script>
</body>
</html>
"""
    return HTMLResponse(html)
