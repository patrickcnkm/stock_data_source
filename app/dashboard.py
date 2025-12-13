from __future__ import annotations

import math
import subprocess
from datetime import date, datetime, timedelta
import math
from typing import List, Optional, Literal

import duckdb
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .deps import get_duck
from .settings import get_settings
from .universe import load_hk_stocks_from_watchlist
from .futu_rate_limit import get_quota_status, reset_quota

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
      - full_last_60d/full_range: All HK stocks from Futu watchlist
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


def _json_safe(value):
    """Convert NaN/Inf to None so responses are JSON serializable."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value




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

    Supported modes:
      - partial: Specify explicit symbol list
      - full_last_60d: All HK stocks from Futu watchlist, last 60 days
      - full_range: All HK stocks from Futu watchlist, custom date range
      - start_date / end_date are OPTIONAL for partial mode:
          * If both provided: use them to derive --days (max 60).
          * If not provided: default to last 2 days.
    """

    # ---- 1. symbols ----
    settings = get_settings()
    allow_partial_commit = settings.allow_partial_commit
    today = date.today()
    yesterday = today - timedelta(days=1)

    if req.mode == "partial":
        if not req.symbols:
            raise HTTPException(status_code=400, detail="symbols is required for partial mode")
        symbols = [_normalize_symbol(s) for s in req.symbols]
    elif req.mode == "full_last_60d":
        # Load HK stocks from watchlist
        symbols = load_hk_stocks_from_watchlist()
        # Last 60 days
        days = 60
        effective_end = yesterday
        effective_start = today - timedelta(days=days)
    elif req.mode == "full_range":
        # Load HK stocks from watchlist
        symbols = load_hk_stocks_from_watchlist()
        # Use provided date range
        if not req.start_date or not req.end_date:
            raise HTTPException(
                status_code=400,
                detail="Both start_date and end_date are required for full_range mode.",
            )
        if req.end_date < req.start_date:
            raise HTTPException(status_code=400, detail="end_date must be >= start_date")
        if req.end_date > yesterday:
            raise HTTPException(status_code=400, detail="end_date must be <= yesterday")
        effective_end = min(req.end_date, yesterday)
        effective_start = req.start_date
        days = (effective_end - effective_start).days + 1
    else:
        raise HTTPException(status_code=400, detail=f"Unknown mode: {req.mode}")

    # ---- 2. compute days based on date range (or default) ----
    if req.mode == "partial":
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
    date_args = ["--start-date", effective_start.isoformat(), "--end-date", effective_end.isoformat()]

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
        ["python", "-m", "workers.ingest_futu", "--symbols", *symbols, *date_args],
    )

    # 2) validate A (vertical)
    run_step(
        "validate_A",
        ["python", "-m", "workers.validate", "--run-a", "--symbols", *symbols, *date_args],
    )

    # 3) validate B (horizontal, demo mode)
    run_step(
        "validate_B_demo",
        ["python", "-m", "workers.validate_cross", "--symbols", *symbols, *date_args, "--demo"],
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

    failed_symbols: set[str] = set()
    if not fail_df.empty:
        for _, row in fail_df.iterrows():
            symbol = row["std_symbol"]
            failed_symbols.add(symbol)
            validation_summary["failures"].append(
                {
                    "symbol": symbol,
                    "file_date": row["file_date"],
                    "rule_name": row["rule_name"],
                    "metric": _json_safe(row["metric"]),
                    "threshold": _json_safe(row["threshold"]),
                }
            )
        if not allow_partial_commit:
            return {
                "status": "validation_failed",
                "plan": plan,
                "steps": steps_log,
                "validation": validation_summary,
            }

    symbols_to_commit = [s for s in symbols if s not in failed_symbols]

    if not symbols_to_commit:
        return {
            "status": "validation_failed",
            "plan": plan,
            "steps": steps_log,
            "validation": validation_summary,
            "message": "No symbols passed validation; nothing committed.",
        }

    if failed_symbols:
        plan.setdefault("skipped_symbols", sorted(failed_symbols))

    # 5) No failures -> commit staging → trusted
    run_step(
        "commit",
        ["python", "-m", "workers.commit", "--symbols", *symbols_to_commit, *date_args],
    )

    return {
        "status": "success" if not failed_symbols else "partial_success",
        "plan": plan,
        "steps": steps_log,
        "validation": validation_summary,
    }


# ============================
# 2) Coverage endpoints
# ============================

@router.get("/api/dashboard/coverage/summary")
def coverage_summary():
    """Return min/max date and total distinct symbols in trusted table."""
    try:
        with get_duck(read_only=True) as duck:
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

    if df.empty or df.iloc[0]["min_date"] is None:
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
def coverage_daily():
    """
    Per-day coverage:
      - trade_date
      - symbol_count
      - coverage: 'full' or 'partial' relative to max symbol_count
    """
    try:
        with get_duck(read_only=True) as duck:
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
                  CASE WHEN daily.symbol_count = max_sym.max_count AND max_sym.max_count > 1
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
def coverage_day_details(trade_date: date):
    """Return symbol list for a specific day in trusted table."""
    try:
        with get_duck(read_only=True) as duck:
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


@router.get("/api/dashboard/ingest/stats")
def ingest_day_stats(trade_date: date):
    """Return ingestion/validation stats for a given trade date."""
    try:
        with get_duck(read_only=True) as duck:
            trusted_df = duck.execute(
                f"""
                SELECT COUNT(DISTINCT std_symbol) AS symbol_count
                FROM {TICKS_TABLE}
                WHERE date_trunc('day', ts_exchange) = ?
                """,
                [trade_date],
            ).fetch_df()

            validation_df = duck.execute(
                """
                SELECT std_symbol, rule_name, passed
                FROM validation_results
                WHERE file_date = ?
                """,
                [trade_date],
            ).fetch_df()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    trusted_symbol_count = int(trusted_df["symbol_count"][0]) if not trusted_df.empty else 0

    processed_symbols: set[str] = set(validation_df["std_symbol"].tolist()) if not validation_df.empty else set()
    failed_df = validation_df[validation_df["passed"] == False] if not validation_df.empty else validation_df
    failed_symbols: set[str] = set(failed_df["std_symbol"].tolist()) if not failed_df.empty else set()
    passed_symbols = processed_symbols - failed_symbols

    error_breakdown = []
    if not failed_df.empty:
        for rule_name, group in failed_df.groupby("rule_name"):
            error_breakdown.append(
                {
                    "rule_name": rule_name,
                    "failed_symbols": sorted(set(group["std_symbol"].tolist())),
                    "failed_symbol_count": int(len(set(group["std_symbol"].tolist()))),
                }
            )

    return {
        "trade_date": trade_date.isoformat(),
        "trusted_symbol_count": trusted_symbol_count,
        "processed_symbol_count": len(processed_symbols),
        "passed_symbol_count": len(passed_symbols),
        "failed_symbol_count": len(failed_symbols),
        "failed_symbols": sorted(failed_symbols),
        "error_breakdown": error_breakdown,
    }


# ============================
# 3) Delete endpoint
# ============================

@router.get("/api/dashboard/quota")
def dashboard_quota_status():
    """Get current Futu API quota status."""
    try:
        return get_quota_status()
    except Exception as e:
        import traceback
        error_detail = str(e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to get quota status: {error_detail}")


@router.post("/api/dashboard/quota/reset")
def dashboard_quota_reset():
    """
    Reset the application's quota tracking counter.
    
    Note: This only resets our application's tracking. If Futu OpenD server
    shows quota reached, you may need to wait for Futu's server-side quota
    to reset (typically resets daily). The application quota auto-resets
    every 24 hours to match Futu's typical reset cycle.
    """
    reset_quota(force=True)
    return {
        "status": "quota_reset",
        "message": "Application quota counter reset. Note: Futu OpenD server quota resets independently (typically daily).",
        "quota": get_quota_status()
    }


@router.post("/api/dashboard/delete")
def dashboard_delete(req: DeleteRequest):
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
        with get_duck(read_only=False) as duck:
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

def _get_last_trading_day() -> str:
    """Get the last trading day (weekday, excluding weekends)."""
    today = date.today()
    last_day = today - timedelta(days=1)
    
    # Go back until we find a weekday (Monday=0, Friday=4)
    while last_day.weekday() >= 5:  # Saturday=5, Sunday=6
        last_day = last_day - timedelta(days=1)
    
    return last_day.isoformat()


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard_page():
    last_trading_day = _get_last_trading_day()
    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>HK In Watch List Tick Data Dashboard</title>
  <style>
    body { font-family: system-ui, -apple-system, BlinkMacSystemFont, sans-serif; margin: 20px; }
    h2 { margin-top: 24px; }
    fieldset { margin-bottom: 16px; }
    pre { background: #111; color: #0f0; padding: 10px; border-radius: 4px; max-height: 260px; overflow: auto; }
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
    .progress {
      width: 100%;
      background: #e5e7eb;
      border-radius: 6px;
      overflow: hidden;
      height: 14px;
      margin-top: 8px;
    }
    .progress-bar {
      height: 100%;
      background: linear-gradient(90deg, #2563eb, #4ade80);
      width: 0%;
      transition: width 0.3s ease;
    }
    .progress-label { font-size: 12px; color: #374151; margin-top: 4px; }
    .error-chip {
      display: inline-block;
      padding: 2px 6px;
      background: #fee2e2;
      color: #b91c1c;
      border-radius: 4px;
      margin: 2px 4px 2px 0;
      font-size: 12px;
    }
  </style>
</head>
<body>
  <h1>HK In Watch List Tick Data Dashboard</h1>

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
        <input type="radio" name="mode" value="full_last_60d">
        Full: All HK stocks in watchlist, last 60 days <span style="color: #666; font-size: 11px;">(Requires Futu OpenD connection)</span>
      </label><br>
      <label>
        <input type="radio" name="mode" value="full_range">
        Full: All HK stocks in watchlist, custom date range <span style="color: #666; font-size: 11px;">(end date must be ≤ yesterday)</span>
      </label>
    </div>

    <div class="flex-row" style="margin-top: 8px;">
      <label>Symbols (comma separated, HK only):</label>
      <input id="ingest-symbols" style="width: 280px;" placeholder="e.g. 00700.HK,00005.HK" value="HK.00700" />
    </div>
    <div class="flex-row" style="margin-top: 8px;">
      <label>Start date (optional):</label>
      <input type="date" id="ingest-start" value="DATE_PLACEHOLDER_START" />
      <label>End date (optional):</label>
      <input type="date" id="ingest-end" value="DATE_PLACEHOLDER_END" />
    </div>

    <div style="margin-top: 8px;">
      <button id="btn-ingest" type="button" class="btn btn-primary">Run Ingestion + Verification</button>
      <button id="btn-stats" type="button" class="btn btn-secondary">Load Ingestion Stats</button>
    </div>
  </fieldset>
  <div class="progress" aria-label="Ingestion progress">
    <div id="ingest-progress-bar" class="progress-bar"></div>
  </div>
  <div id="ingest-progress-text" class="progress-label">Idle</div>
  <pre id="ingest-log">[Ready]</pre>
  <div id="ingest-stats" style="font-size: 13px; margin-top: 8px;"></div>

  <!-- 2. Coverage Overview -->
  <h2>2. Ingested Data Overview</h2>
  <button id="btn-coverage" type="button" class="btn btn-secondary">Reload Coverage</button>
  <div id="coverage-summary" style="margin-top: 8px; font-size: 13px;"></div>
  <div id="coverage-table-wrapper" style="margin-top: 8px;"></div>

  <!-- 2.5. Futu API Quota Status -->
  <h2>2.5. Futu API Quota Status</h2>
  <div style="margin-top: 8px;">
    <button id="btn-quota" type="button" class="btn btn-secondary">Refresh Quota Status</button>
    <button id="btn-quota-reset" type="button" class="btn btn-danger" style="margin-left: 8px;">Reset Total Quota</button>
  </div>
  <div id="quota-status" style="margin-top: 8px; font-size: 13px; padding: 8px; background: #f5f5f5; border-radius: 4px;"></div>

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
      <button id="btn-delete" type="button" class="btn btn-danger">Delete</button>
    </div>
  </fieldset>
  <pre id="delete-log">[No delete run]</pre>

<script>
console.log('[SCRIPT] Starting script execution...');
// Define functions IMMEDIATELY (outside DOMContentLoaded) so they're available globally
// Define helper functions first
window.setIngestProgress = function setIngestProgress(percent, label) {
  const bar = document.getElementById('ingest-progress-bar');
  const text = document.getElementById('ingest-progress-text');
  if (bar) bar.style.width = `${Math.min(100, Math.max(0, percent))}%`;
  if (text) text.textContent = label;
};
console.log('[SCRIPT] setIngestProgress defined on window:', typeof window.setIngestProgress);

// Immediately attach functions to window BEFORE DOMContentLoaded
// This ensures they're defined before the event listeners are attached
window.triggerIngest = async function triggerIngest() {
    console.log('[FUNCTION] triggerIngest called - function exists!');
    console.log('========================================');
    console.log('[BUTTON CLICKED] Run Ingestion + Verification');
    console.log('========================================');
    
    // Immediate UI feedback: disable button and show "Running..."
    const btnIngest = document.getElementById('btn-ingest');
    const originalBtnText = btnIngest ? btnIngest.textContent : 'Run Ingestion + Verification';
    if (btnIngest) {
      btnIngest.disabled = true;
      btnIngest.textContent = 'Running...';
    }
    
    const mode = document.querySelector('input[name="mode"]:checked').value;
    const symbolsRaw = document.getElementById('ingest-symbols').value.trim();
    const start = document.getElementById('ingest-start').value;
    const end = document.getElementById('ingest-end').value;

    console.log('[DEBUG] Mode selected:', mode);
    console.log('[DEBUG] Symbols (raw):', symbolsRaw);
    console.log('[DEBUG] Start date:', start);
    console.log('[DEBUG] End date:', end);

  const body = { mode: mode };

  if (mode === 'partial') {
      console.log('[DEBUG] Processing PARTIAL mode');
    body.symbols = symbolsRaw ? symbolsRaw.split(',').map(s => s.trim()).filter(Boolean) : [];
      console.log('[DEBUG] Symbols (parsed):', body.symbols);
    if (start && end) {
      body.start_date = start;
      body.end_date = end;
        console.log('[DEBUG] Date range provided:', start, 'to', end);
    } else {
      body.start_date = null;
      body.end_date = null;
        console.log('[DEBUG] No date range provided, using default (last 2 days)');
    }
      console.log('[DEBUG] System: Partial ingestion - specific stocks with optional date range');
  } else if (mode === 'full_last_60d') {
      console.log('[DEBUG] Processing FULL_LAST_60D mode');
    body.symbols = null;
    body.start_date = null;
    body.end_date = null;
      console.log('[DEBUG] System: Full ingestion - all HK stocks from watchlist, last 60 days');
  } else if (mode === 'full_range') {
      console.log('[DEBUG] Processing FULL_RANGE mode');
    if (start && end) {
      body.start_date = start;
      body.end_date = end;
        console.log('[DEBUG] Date range provided:', start, 'to', end);
    } else {
      body.start_date = null;
      body.end_date = null;
        console.log('[DEBUG] No date range provided');
    }
    body.symbols = null;
      console.log('[DEBUG] System: Full ingestion - all HK stocks from watchlist, custom date range');
  }
    
    console.log('[DEBUG] Request body:', JSON.stringify(body, null, 2));

    console.log('[DEBUG] Preparing to send request to /api/dashboard/ingest');

    const log = document.getElementById('ingest-log');
    if (log) log.textContent = '[Running] Sending request...';
    if (typeof window.setIngestProgress === 'function') {
      window.setIngestProgress(10, 'Starting pipeline...');
    }

    try {
      console.log('[DEBUG] Sending POST request...');
      const res = await fetch('/api/dashboard/ingest', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body)
      });
      console.log('[DEBUG] Response status:', res.status, res.statusText);
      if (typeof window.setIngestProgress === 'function') {
        window.setIngestProgress(35, 'Pipeline running...');
      }
      
      // Harden response parsing: handle non-JSON responses gracefully
      let data;
      const responseText = await res.text();
      console.log('[DEBUG] Response text received (first 500 chars):', responseText.substring(0, 500));
      
      if (!res.ok) {
        // HTTP error (4xx, 5xx)
        const errorMsg = `HTTP ${res.status} ${res.statusText}\n\nResponse body:\n${responseText}`;
        console.error('[ERROR] HTTP error response:', errorMsg);
        if (log) log.textContent = errorMsg;
        if (typeof window.setIngestProgress === 'function') {
          window.setIngestProgress(0, `Error: HTTP ${res.status}`);
        }
        // Re-enable button on error
        if (btnIngest) {
          btnIngest.disabled = false;
          btnIngest.textContent = originalBtnText;
        }
        return;
      }
      
      try {
        data = JSON.parse(responseText);
        console.log('[DEBUG] Response data parsed successfully:', data);
      } catch (parseError) {
        // Response is not valid JSON
        const errorMsg = `Server returned non-JSON response (status: ${res.status})\n\nResponse body:\n${responseText}`;
        console.error('[ERROR] Failed to parse JSON:', parseError);
        console.error('[ERROR] Response text:', responseText);
        if (log) log.textContent = errorMsg;
        if (typeof window.setIngestProgress === 'function') {
          window.setIngestProgress(0, 'Error: Invalid server response');
        }
        // Re-enable button on error
        if (btnIngest) {
          btnIngest.disabled = false;
          btnIngest.textContent = originalBtnText;
        }
        return;
      }
      
      if (log) log.textContent = JSON.stringify(data, null, 2);

    // Update progress based on completed steps
    const steps = data.steps || [];
    if (steps.length) {
      const completed = steps.filter(s => s.status === 'ok').length;
      const pct = Math.max(35, Math.round((completed / steps.length) * 100));
      const label = data.status === 'success' ? 'Completed' : data.status === 'partial_success' ? 'Partial commit' : 'Completed with validation failures';
        if (typeof window.setIngestProgress === 'function') {
          window.setIngestProgress(pct, label);
        }
    } else {
        if (typeof window.setIngestProgress === 'function') {
          window.setIngestProgress(100, 'Completed');
        }
    }

      // Reload coverage after successful ingestion
      console.log('[DEBUG] Ingestion completed, reloading coverage...');
      await window.loadCoverage();
      console.log('[DEBUG] All actions completed successfully');
      
      // Re-enable button on success
      if (btnIngest) {
        btnIngest.disabled = false;
        btnIngest.textContent = originalBtnText;
      }
    } catch (e) {
      console.error('[ERROR] Error in triggerIngest:', e);
      console.error('[ERROR] Error stack:', e.stack);
      if (log) log.textContent = 'Error: ' + e + (e.stack ? '\\n\\nStack trace:\\n' + e.stack : '');
      if (typeof window.setIngestProgress === 'function') {
        window.setIngestProgress(0, 'Error running pipeline');
      }
      // Re-enable button on error
      if (btnIngest) {
        btnIngest.disabled = false;
        btnIngest.textContent = originalBtnText;
      }
    }
    console.log('========================================');
  };


window.loadIngestStats = async function loadIngestStats(tradeDateOverride) {
  console.log('========================================');
  console.log('[BUTTON CLICKED] Load Ingestion Stats');
  console.log('========================================');
  
  const statsDiv = document.getElementById('ingest-stats');
  const date = tradeDateOverride || document.getElementById('ingest-end').value;
  console.log('[DEBUG] Trade date override:', tradeDateOverride);
  console.log('[DEBUG] Selected date:', date);
  
  if (!date) {
    console.log('[DEBUG] No date selected, showing message');
    statsDiv.textContent = 'Select a date to load ingestion stats.';
    return;
  }

  console.log('[DEBUG] Loading stats for date:', date);
  console.log('[DEBUG] System: Querying ingestion stats from database');
  statsDiv.textContent = 'Loading ingestion stats...';

  try {
    console.log('[DEBUG] Sending GET request to /api/dashboard/ingest/stats');
    const url = `/api/dashboard/ingest/stats?trade_date=${date}`;
    console.log('[DEBUG] Request URL:', url);
    const res = await fetch(url);
    console.log('[DEBUG] Response status:', res.status);
    const data = await res.json();
    console.log('[DEBUG] Stats data received:', data);

    const errors = data.error_breakdown || [];
    const errorHtml = errors.length
      ? errors.map(e => `<div class="error-chip">${e.rule_name}: ${e.failed_symbol_count} symbol(s)</div>`).join('')
      : '<span style="color:#16a34a">No validation errors recorded.</span>';

    statsDiv.innerHTML = `
      <div><b>${data.trade_date}</b> ingestion</div>
      <ul style="margin-top:4px;">
        <li>Distinct symbols committed: <b>${data.trusted_symbol_count}</b></li>
        <li>Symbols processed (validated): <b>${data.processed_symbol_count}</b></li>
        <li>Passed verification: <b>${data.passed_symbol_count}</b></li>
        <li>Failed verification: <b>${data.failed_symbol_count}</b></li>
      </ul>
      <div style="margin-top:4px;">Error breakdown: ${errorHtml}</div>
    `;
    console.log('[DEBUG] Stats displayed successfully');
  } catch (e) {
    console.error('[ERROR] Error in loadIngestStats:', e);
    console.error('[ERROR] Error stack:', e.stack);
    statsDiv.textContent = 'Error loading ingestion stats: ' + e;
  }
  console.log('========================================');
  };
  console.log('[SCRIPT] loadIngestStats defined on window:', typeof window.loadIngestStats);

  window.loadCoverage = async function loadCoverage() {
  console.log('========================================');
  console.log('[BUTTON CLICKED] Reload Coverage');
  console.log('========================================');
  console.log('[DEBUG] System: Querying coverage data from database');
  
  const summaryDiv = document.getElementById('coverage-summary');
  const tableDiv = document.getElementById('coverage-table-wrapper');
  summaryDiv.textContent = 'Loading...';
  tableDiv.innerHTML = '';

  try {
    console.log('[DEBUG] Sending parallel requests for coverage summary and daily data');
    const [resSum, resDaily] = await Promise.all([
      fetch('/api/dashboard/coverage/summary'),
      fetch('/api/dashboard/coverage/daily'),
    ]);
    console.log('[DEBUG] Coverage summary response status:', resSum.status);
    console.log('[DEBUG] Coverage daily response status:', resDaily.status);
    const sum = await resSum.json();
    const daily = await resDaily.json();
    console.log('[DEBUG] Coverage summary data:', sum);
    console.log('[DEBUG] Coverage daily data:', daily);

    if (!sum.has_data) {
      if (summaryDiv) summaryDiv.textContent = 'No data in fact_ticks_trusted yet.';
      return;
    }

    if (summaryDiv) {
    summaryDiv.innerHTML = `
      Date range: <b>${sum.min_date}</b> ~ <b>${sum.max_date}</b><br/>
      Total distinct symbols: <b>${sum.total_symbols}</b>
    `;
    }

    const days = daily.days || [];
    if (!days.length) {
      if (tableDiv) tableDiv.textContent = 'No daily coverage rows.';
      return;
    }

    let html = '<table><thead><tr><th>Trade Date</th><th>Symbol Count</th><th>Coverage</th></tr></thead><tbody>';
    for (const d of days) {
      const cls = d.coverage === 'full' ? 'full' : 'partial';
      html += `<tr class="${cls}"><td>${d.trade_date}</td><td>${d.symbol_count}</td><td>${d.coverage}</td></tr>`;
    }
    html += '</tbody></table>';
    tableDiv.innerHTML = html;
    console.log('[DEBUG] Coverage data displayed successfully');
  } catch (e) {
    console.error('[ERROR] Error in loadCoverage:', e);
    console.error('[ERROR] Error stack:', e.stack);
    summaryDiv.textContent = 'Error loading coverage: ' + e;
  }
  console.log('========================================');
  };
  console.log('[SCRIPT] loadCoverage defined on window:', typeof window.loadCoverage);

  window.runDelete = async function runDelete() {
  console.log('========================================');
  console.log('[BUTTON CLICKED] Delete');
  console.log('========================================');
  
  const symbolsRaw = document.getElementById('del-symbols').value.trim();
  const start = document.getElementById('del-start').value;
  const end = document.getElementById('del-end').value;
  const log = document.getElementById('delete-log');

  console.log('[DEBUG] Symbols to delete (raw):', symbolsRaw);
  console.log('[DEBUG] Start date:', start);
  console.log('[DEBUG] End date:', end);

  const body = {};
  if (symbolsRaw) {
    body.symbols = symbolsRaw.split(',').map(s => s.trim()).filter(Boolean);
    console.log('[DEBUG] Symbols to delete (parsed):', body.symbols);
  }
  if (start && end) {
    body.start_date = start;
    body.end_date = end;
    console.log('[DEBUG] Date range:', start, 'to', end);
  }

  console.log('[DEBUG] System: Deleting from fact_ticks_trusted table');
  console.log('[DEBUG] Request body:', JSON.stringify(body, null, 2));
  log.textContent = '[Running] Deleting...';

  try {
    console.log('[DEBUG] Sending DELETE request to /api/dashboard/delete');
    const res = await fetch('/api/dashboard/delete', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    console.log('[DEBUG] Response status:', res.status);
    const data = await res.json();
    console.log('[DEBUG] Delete result:', data);
    log.textContent = JSON.stringify(data, null, 2);
    console.log('[DEBUG] Delete operation completed');
  } catch (e) {
    console.error('[ERROR] Error in runDelete:', e);
    console.error('[ERROR] Error stack:', e.stack);
    log.textContent = 'Error: ' + e;
  }
  console.log('========================================');
  };
  console.log('[SCRIPT] runDelete defined on window:', typeof window.runDelete);

  window.loadQuotaStatus = async function loadQuotaStatus() {
  console.log('========================================');
  console.log('[BUTTON CLICKED] Refresh Quota Status');
  console.log('========================================');
  console.log('[DEBUG] System: Querying Futu API quota status');
  
  const statusDiv = document.getElementById('quota-status');
  statusDiv.textContent = 'Loading quota status...';

  try {
    console.log('[DEBUG] Sending GET request to /api/dashboard/quota');
    const res = await fetch('/api/dashboard/quota');
    console.log('[DEBUG] Response status:', res.status, res.statusText);
    
    // Harden response parsing: handle non-JSON responses gracefully
    let data;
    const responseText = await res.text();
    console.log('[DEBUG] Response text received (first 500 chars):', responseText.substring(0, 500));
    
    if (!res.ok) {
      // HTTP error (4xx, 5xx)
      const errorMsg = `HTTP ${res.status} ${res.statusText}\n\nResponse body:\n${responseText}`;
      console.error('[ERROR] HTTP error response:', errorMsg);
      statusDiv.textContent = errorMsg;
      return;
    }
    
    try {
      data = JSON.parse(responseText);
      console.log('[DEBUG] Quota data parsed successfully:', data);
    } catch (parseError) {
      // Response is not valid JSON
      const errorMsg = `Server returned non-JSON response (status: ${res.status})\n\nResponse body:\n${responseText}`;
      console.error('[ERROR] Failed to parse JSON:', parseError);
      console.error('[ERROR] Response text:', responseText);
      statusDiv.textContent = errorMsg;
      return;
    }

    const pct30s = Math.round((data.requests_in_30s_window / 60) * 100);
    const pctTotal = Math.round((data.total_requests / 300) * 100);
    const color30s = pct30s >= 90 ? '#dc2626' : pct30s >= 70 ? '#f59e0b' : '#16a34a';
    const colorTotal = pctTotal >= 90 ? '#dc2626' : pctTotal >= 70 ? '#f59e0b' : '#16a34a';

    const hoursUntilReset = Math.floor((data.time_until_auto_reset_seconds || 0) / 3600);
    const minutesUntilReset = Math.floor(((data.time_until_auto_reset_seconds || 0) % 3600) / 60);

    statusDiv.innerHTML = `
      <div><b>30-Second Window:</b> ${data.requests_in_30s_window} / 60 requests (${data.remaining_30s_quota} remaining)
        <div style="width: 100%; background: #e5e7eb; border-radius: 4px; height: 20px; margin-top: 4px;">
          <div style="width: ${pct30s}%; background: ${color30s}; height: 100%; border-radius: 4px; transition: width 0.3s;"></div>
        </div>
      </div>
      <div style="margin-top: 12px;"><b>Application Quota (Tracking):</b> ${data.total_requests} / 300 requests (${data.remaining_total_quota} remaining)
        <div style="width: 100%; background: #e5e7eb; border-radius: 4px; height: 20px; margin-top: 4px;">
          <div style="width: ${pctTotal}%; background: ${colorTotal}; height: 100%; border-radius: 4px; transition: width 0.3s;"></div>
        </div>
      </div>
      <div style="margin-top: 8px; font-size: 11px; color: #666;">
        Last reset: ${new Date(data.quota_reset_time * 1000).toLocaleString()}<br/>
        Auto-reset in: ${hoursUntilReset}h ${minutesUntilReset}m
      </div>
      <div style="margin-top: 8px; padding: 8px; background: #fef3c7; border-left: 3px solid #f59e0b; font-size: 11px; color: #92400e;">
        <b>⚠ Important:</b> ${data.note ? data.note.replace(/"/g, '&quot;') : "This tracks application usage. Futu OpenD server has its own quota tracking that resets independently (typically daily). If Futu OpenD shows quota reached, you may need to wait for Futu's server-side quota to reset."}
      </div>
    `;
    console.log('[DEBUG] Quota status displayed successfully');
  } catch (e) {
    console.error('[ERROR] Error in loadQuotaStatus:', e);
    console.error('[ERROR] Error stack:', e.stack);
    statusDiv.textContent = 'Error loading quota status: ' + e;
  }
  console.log('========================================');
  };
  console.log('[SCRIPT] loadQuotaStatus defined on window:', typeof window.loadQuotaStatus);

  window.resetQuota = async function resetQuota() {
  console.log('========================================');
  console.log('[BUTTON CLICKED] Reset Total Quota');
  console.log('========================================');
  console.log('[DEBUG] System: Resetting application quota tracking counter');
  
  const msg = 'Reset the application quota counter?\\n\\n' +
              'Note: This only resets our application\\'s tracking. If Futu OpenD server ' +
              'also shows quota reached, you may need to wait for Futu\\'s server-side quota ' +
              'to reset (typically resets daily).';
  if (!confirm(msg)) {
    console.log('[DEBUG] User cancelled quota reset');
    return;
  }

  console.log('[DEBUG] User confirmed quota reset');
  const statusDiv = document.getElementById('quota-status');
  statusDiv.textContent = 'Resetting quota...';

  try {
    console.log('[DEBUG] Sending POST request to /api/dashboard/quota/reset');
    const res = await fetch('/api/dashboard/quota/reset', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'}
    });
    console.log('[DEBUG] Response status:', res.status);
    const data = await res.json();
    console.log('[DEBUG] Reset response:', data);
    if (data.message) {
      console.log('[DEBUG] Showing alert message:', data.message);
      alert(data.message);
    }
    console.log('[DEBUG] Reloading quota status after reset...');
    await window.loadQuotaStatus();
    console.log('[DEBUG] Quota reset completed successfully');
  } catch (e) {
    console.error('[ERROR] Error in resetQuota:', e);
    console.error('[ERROR] Error stack:', e.stack);
    statusDiv.textContent = 'Error resetting quota: ' + e;
  }
  console.log('========================================');
  };
console.log('[SCRIPT] resetQuota defined on window:', typeof window.resetQuota);
console.log('[SCRIPT] All functions defined. Final check:');
console.log('[SCRIPT] - triggerIngest:', typeof window.triggerIngest);
console.log('[SCRIPT] - loadIngestStats:', typeof window.loadIngestStats);
console.log('[SCRIPT] - loadCoverage:', typeof window.loadCoverage);
console.log('[SCRIPT] - runDelete:', typeof window.runDelete);
console.log('[SCRIPT] - loadQuotaStatus:', typeof window.loadQuotaStatus);
console.log('[SCRIPT] - resetQuota:', typeof window.resetQuota);

// Wait for DOM to be ready before initializing event listeners
document.addEventListener('DOMContentLoaded', function() {
  console.log('[DOM] DOMContentLoaded fired - DOM is ready');
  
  try {
    // Initialize button event listeners - INSIDE DOMContentLoaded
    console.log('[INIT] Dashboard JavaScript loaded');
  console.log('[INIT] Checking if all functions are available...');
  
  // Verify all functions are available
  const functions = ['triggerIngest', 'loadIngestStats', 'loadCoverage', 'runDelete', 'loadQuotaStatus', 'resetQuota'];
  let allAvailable = true;
  for (const funcName of functions) {
    if (typeof window[funcName] === 'function') {
      console.log('[INIT] ✓', funcName, 'is available');
    } else {
      console.error('[INIT] ✗', funcName, 'is NOT available!');
      allAvailable = false;
    }
  }
  
  if (allAvailable) {
    console.log('[INIT] All functions are available and ready');
  } else {
    console.error('[INIT] ERROR: Some functions are missing!');
    alert('ERROR: Some JavaScript functions are missing. Please check the console.');
  }
  
  // Attach event listeners to buttons
  console.log('[INIT] Attaching event listeners to buttons...');
  
  const btnIngest = document.getElementById('btn-ingest');
  if (btnIngest) {
    btnIngest.addEventListener('click', function() {
      console.log('[BUTTON] btn-ingest clicked');
      if (typeof window.triggerIngest === 'function') {
        window.triggerIngest();
      } else {
        alert('Error: triggerIngest function not found. Check console.');
        console.error('triggerIngest not available, type:', typeof window.triggerIngest);
      }
    });
    console.log('[INIT] ✓ btn-ingest listener attached');
  } else {
    console.error('[INIT] ✗ btn-ingest button not found in DOM!');
  }
  
  const btnStats = document.getElementById('btn-stats');
  if (btnStats) {
    btnStats.addEventListener('click', function() {
      console.log('[BUTTON] btn-stats clicked');
      if (typeof window.loadIngestStats === 'function') {
        window.loadIngestStats();
      } else {
        alert('Error: loadIngestStats function not found.');
      }
    });
    console.log('[INIT] ✓ btn-stats listener attached');
  } else {
    console.error('[INIT] ✗ btn-stats button not found in DOM!');
  }
  
  const btnCoverage = document.getElementById('btn-coverage');
  if (btnCoverage) {
    btnCoverage.addEventListener('click', function() {
      console.log('[BUTTON] btn-coverage clicked');
      if (typeof window.loadCoverage === 'function') {
        window.loadCoverage();
      } else {
        alert('Error: loadCoverage function not found.');
      }
    });
    console.log('[INIT] ✓ btn-coverage listener attached');
  } else {
    console.error('[INIT] ✗ btn-coverage button not found in DOM!');
  }
  
  const btnQuota = document.getElementById('btn-quota');
  if (btnQuota) {
    btnQuota.addEventListener('click', function() {
      console.log('[BUTTON] btn-quota clicked');
      if (typeof window.loadQuotaStatus === 'function') {
        window.loadQuotaStatus();
      } else {
        alert('Error: loadQuotaStatus function not found.');
      }
    });
    console.log('[INIT] ✓ btn-quota listener attached');
  } else {
    console.error('[INIT] ✗ btn-quota button not found in DOM!');
  }
  
  const btnQuotaReset = document.getElementById('btn-quota-reset');
  if (btnQuotaReset) {
    btnQuotaReset.addEventListener('click', function() {
      console.log('[BUTTON] btn-quota-reset clicked');
      if (typeof window.resetQuota === 'function') {
        window.resetQuota();
      } else {
        alert('Error: resetQuota function not found.');
      }
    });
    console.log('[INIT] ✓ btn-quota-reset listener attached');
  } else {
    console.error('[INIT] ✗ btn-quota-reset button not found in DOM!');
  }
  
  const btnDelete = document.getElementById('btn-delete');
  if (btnDelete) {
    btnDelete.addEventListener('click', function() {
      console.log('[BUTTON] btn-delete clicked');
      if (typeof window.runDelete === 'function') {
        window.runDelete();
      } else {
        alert('Error: runDelete function not found.');
      }
    });
    console.log('[INIT] ✓ btn-delete listener attached');
  } else {
    console.error('[INIT] ✗ btn-delete button not found in DOM!');
  }
  
    console.log('[INIT] All event listeners attached');
    
    // Load quota status on page load
    if (document.getElementById('quota-status')) {
      window.loadQuotaStatus();
    }
    
    console.log('[INIT] All initialization complete');
  } catch (error) {
    console.error('[INIT] ERROR during DOMContentLoaded:', error);
    console.error('[INIT] Error stack:', error.stack);
    alert('JavaScript error during initialization: ' + error.message + '\\nCheck console for details.');
  }
}); // End of DOMContentLoaded handler

</script>
</body>
</html>
"""
    # Replace date placeholders
    html = html.replace("DATE_PLACEHOLDER_START", last_trading_day)
    html = html.replace("DATE_PLACEHOLDER_END", last_trading_day)
    return HTMLResponse(html)
