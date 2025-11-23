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
from .universe import load_hk_universe

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

    Current limitations:
      - Only mode=partial is supported (explicit symbol list).
      - start_date / end_date are OPTIONAL:
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
        # Load HK universe
        symbols = load_hk_universe()
        if not symbols:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No HK universe symbols found. "
                    "Set HK_UNIVERSE_SYMBOLS env var or edit data/hk_universe_default.txt."
                ),
            )
        # Last 60 days
        days = 60
        effective_end = yesterday
        effective_start = today - timedelta(days=days)
    elif req.mode == "full_range":
        # Load HK universe
        symbols = load_hk_universe()
        if not symbols:
            raise HTTPException(
                status_code=400,
                detail=(
                    "No HK universe symbols found. "
                    "Set HK_UNIVERSE_SYMBOLS env var or edit data/hk_universe_default.txt."
                ),
            )
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
        <input type="radio" name="mode" value="full_last_60d">
        Full: All HK stocks, last 60 days <span style="color: #666; font-size: 11px;">(Requires HK_UNIVERSE_SYMBOLS env var)</span>
      </label><br>
      <label>
        <input type="radio" name="mode" value="full_range">
        Full: All HK stocks, custom date range <span style="color: #666; font-size: 11px;">(end date must be ≤ yesterday)</span>
      </label>
    </div>

    <div class="flex-row" style="margin-top: 8px;">
      <label>Symbols (comma separated, HK only):</label>
      <input id="ingest-symbols" style="width: 280px;" placeholder="e.g. 00700.HK,00005.HK" value="HK.00700" />
    </div>
    <div class="flex-row" style="margin-top: 8px;">
      <label>Start date (optional):</label>
      <input type="date" id="ingest-start" />
      <label>End date (optional):</label>
      <input type="date" id="ingest-end" />
    </div>

    <div style="margin-top: 8px;">
      <button id="btn-ingest" type="button" class="btn btn-primary" onclick="triggerIngest()">Run Ingestion + Verification</button>
      <button id="btn-stats" type="button" class="btn btn-secondary" onclick="loadIngestStats()">Load Ingestion Stats</button>
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
  <button id="btn-coverage" type="button" class="btn btn-secondary" onclick="loadCoverage()">Reload Coverage</button>
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
      <button id="btn-delete" type="button" class="btn btn-danger" onclick="runDelete()">Delete</button>
    </div>
  </fieldset>
  <pre id="delete-log">[No delete run]</pre>

<script>
// Set default date to yesterday
(function() {
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const dateStr = yesterday.toISOString().split('T')[0];
  document.getElementById('ingest-start').value = dateStr;
  document.getElementById('ingest-end').value = dateStr;
})();

// Ensure functions are globally accessible
window.triggerIngest = async function triggerIngest() {
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
  } else if (mode === 'full_last_60d') {
    // Full mode: last 60 days, no symbols or dates needed
    body.symbols = null;
    body.start_date = null;
    body.end_date = null;
  } else if (mode === 'full_range') {
    // Full mode: custom date range
    if (start && end) {
      body.start_date = start;
      body.end_date = end;
    } else {
      body.start_date = null;
      body.end_date = null;
    }
    body.symbols = null;
  }

  const log = document.getElementById('ingest-log');
  log.textContent = '[Running] Sending request...';
  setIngestProgress(10, 'Starting pipeline...');

  try {
    const res = await fetch('/api/dashboard/ingest', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    setIngestProgress(35, 'Pipeline running...');
    const data = await res.json();
    log.textContent = JSON.stringify(data, null, 2);

    // Update progress based on completed steps
    const steps = data.steps || [];
    if (steps.length) {
      const completed = steps.filter(s => s.status === 'ok').length;
      const pct = Math.max(35, Math.round((completed / steps.length) * 100));
      const label = data.status === 'success' ? 'Completed' : data.status === 'partial_success' ? 'Partial commit' : 'Completed with validation failures';
      setIngestProgress(pct, label);
    } else {
      setIngestProgress(100, 'Completed');
    }

    // Reload coverage after successful ingestion
    await loadCoverage();
  } catch (e) {
    log.textContent = 'Error: ' + e;
    setIngestProgress(0, 'Error running pipeline');
  }
};

window.setIngestProgress = function setIngestProgress(percent, label) {
  const bar = document.getElementById('ingest-progress-bar');
  const text = document.getElementById('ingest-progress-text');
  bar.style.width = `${Math.min(100, Math.max(0, percent))}%`;
  text.textContent = label;
};

window.loadIngestStats = async function loadIngestStats(tradeDateOverride) {
  const statsDiv = document.getElementById('ingest-stats');
  const date = tradeDateOverride || document.getElementById('ingest-end').value;
  if (!date) {
    statsDiv.textContent = 'Select a date to load ingestion stats.';
    return;
  }

  statsDiv.textContent = 'Loading ingestion stats...';

  try {
    const res = await fetch(`/api/dashboard/ingest/stats?trade_date=${date}`);
    const data = await res.json();

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
  } catch (e) {
    statsDiv.textContent = 'Error loading ingestion stats: ' + e;
  }
};

window.loadCoverage = async function loadCoverage() {
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
};

window.runDelete = async function runDelete() {
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

// Attach event listeners when DOM is ready (backup to onclick handlers)
function attachButtonListeners() {
  const ingestBtn = document.getElementById('btn-ingest');
  const statsBtn = document.getElementById('btn-stats');
  const coverageBtn = document.getElementById('btn-coverage');
  const deleteBtn = document.getElementById('btn-delete');
  
  if (ingestBtn) ingestBtn.addEventListener('click', triggerIngest);
  if (statsBtn) statsBtn.addEventListener('click', loadIngestStats);
  if (coverageBtn) coverageBtn.addEventListener('click', loadCoverage);
  if (deleteBtn) deleteBtn.addEventListener('click', runDelete);
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', attachButtonListeners);
} else {
  // DOM already loaded, attach immediately
  attachButtonListeners();
}
</script>
</body>
</html>
"""
    return HTMLResponse(html)
