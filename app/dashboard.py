from __future__ import annotations

import logging
import math
import subprocess
import threading
import time
from datetime import date, datetime, timedelta
from typing import List, Optional, Literal, Dict

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .deps import get_duck
from .settings import get_settings
# Lazy import for universe to avoid hanging during module import
from .futu_rate_limit import get_quota_status, reset_quota

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory store for real-time ingestion status
_ingestion_status: Dict[str, dict] = {}
_status_lock = threading.Lock()

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

@router.get("/api/dashboard/ingest/status")
def dashboard_ingest_status(status_id: Optional[str] = Query(None, description="Optional status ID to filter results")):
    """Get real-time ingestion status."""
    with _status_lock:
        if status_id:
            # Return specific status if requested
            return {status_id: _ingestion_status.get(status_id, {})}
        # Return all statuses
        return _ingestion_status.copy()


@router.post("/api/dashboard/ingest/check")
def dashboard_ingest_check(req: IngestRequest):
    """
    Check if items to be ingested already exist in the database.
    Returns list of existing symbol-date pairs.
    """
    today = date.today()
    yesterday = today - timedelta(days=1)

    if req.mode == "partial":
        if not req.symbols:
            raise HTTPException(status_code=400, detail="symbols is required for partial mode")
        symbols = [_normalize_symbol(s) for s in req.symbols]
    elif req.mode == "full_last_60d":
        from .universe import load_hk_stocks_from_watchlist
        symbols = load_hk_stocks_from_watchlist()
        days = 60
        effective_end = yesterday
        effective_start = yesterday - timedelta(days=59)
    elif req.mode == "full_range":
        from .universe import load_hk_stocks_from_watchlist
        symbols = load_hk_stocks_from_watchlist()
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

    if req.mode == "partial":
        if req.start_date and req.end_date:
            if req.end_date < req.start_date:
                raise HTTPException(status_code=400, detail="end_date must be >= start_date")
            if req.end_date > yesterday:
                raise HTTPException(status_code=400, detail="end_date must be <= yesterday")
            effective_end = min(req.end_date, yesterday)
            effective_start = req.start_date
            days = (effective_end - effective_start).days + 1
        elif not req.start_date and not req.end_date:
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

    if days > 60:
        days = 60
        effective_end = yesterday
        effective_start = yesterday - timedelta(days=days - 1)

    date_list = [effective_start + timedelta(days=i) for i in range(days)]
    date_list_iso = [d.isoformat() for d in date_list]

    if not symbols:
        return {
            "has_existing": False,
            "existing_items": [],
            "total_items": 0,
        }

    # Check for existing items in trusted table
    try:
        with get_duck(read_only=True) as duck:
            placeholders = ",".join(["?"] * len(symbols))
            date_placeholders = ",".join(["?"] * len(date_list_iso))
            
            existing_df = duck.execute(
                f"""
                SELECT DISTINCT std_symbol, date_trunc('day', ts_exchange)::DATE AS trade_date
                FROM {TICKS_TABLE}
                WHERE std_symbol IN ({placeholders})
                  AND date_trunc('day', ts_exchange) IN ({date_placeholders})
                ORDER BY std_symbol, trade_date
                """,
                symbols + date_list_iso,
            ).fetch_df()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error checking existing items: {e}")

    existing_items = []
    if not existing_df.empty:
        for _, row in existing_df.iterrows():
            existing_items.append({
                "symbol": row["std_symbol"],
                "date": row["trade_date"].isoformat() if hasattr(row["trade_date"], "isoformat") else str(row["trade_date"]),
            })

    total_items = len(symbols) * len(date_list)
    
    return {
        "has_existing": len(existing_items) > 0,
        "existing_items": existing_items,
        "total_items": total_items,
        "existing_count": len(existing_items),
    }


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
    import traceback
    import uuid
    
    # Generate unique status ID for this ingestion run
    status_id = str(uuid.uuid4())
    
    # Initialize status
    with _status_lock:
        _ingestion_status[status_id] = {
            "status": "starting",
            "current_step": None,
            "current_symbol": None,
            "current_date": None,
            "message": "Initializing ingestion...",
            "start_time": time.time(),
            "detailed_progress": [],  # List of {symbol, date, status: ingest/validate/commit}
            "total_watchlist_items": 0,
        }
    
    try:
        logger.info(f"[INGEST] Starting ingestion (status_id={status_id}, mode={req.mode})")
        
        # ---- 1. symbols ----
        settings = get_settings()
        allow_partial_commit = settings.allow_partial_commit
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        with _status_lock:
            _ingestion_status[status_id]["message"] = "Loading symbols..."
        
        logger.info(f"[INGEST] Loading symbols for mode: {req.mode}")

        if req.mode == "partial":
            if not req.symbols:
                raise HTTPException(status_code=400, detail="symbols is required for partial mode")
            symbols = [_normalize_symbol(s) for s in req.symbols]
        elif req.mode == "full_last_60d":
            # Load HK stocks from watchlist (lazy import)
            try:
                from .universe import load_hk_stocks_from_watchlist
                symbols = load_hk_stocks_from_watchlist()
                if not symbols:
                    raise HTTPException(
                        status_code=400,
                        detail="No HK stocks found in watchlist. Please ensure Futu OpenD is connected and your watchlist contains HK stocks.",
                    )
            except ImportError as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to import universe module: {e}",
                )
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to load watchlist: {str(e)}",
                )
            # For full_last_60d, we'll process each symbol with its own date range
            # This will be handled in the ingestion loop below
            # Set a default range for planning purposes
            days = 60
            effective_end = yesterday
            effective_start = yesterday - timedelta(days=59)
        elif req.mode == "full_range":
            # Load HK stocks from watchlist (lazy import)
            try:
                from .universe import load_hk_stocks_from_watchlist
                symbols = load_hk_stocks_from_watchlist()
                if not symbols:
                    raise HTTPException(
                        status_code=400,
                        detail="No HK stocks found in watchlist. Please ensure Futu OpenD is connected and your watchlist contains HK stocks.",
                    )
            except ImportError as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to import universe module: {e}",
                )
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to load watchlist: {str(e)}",
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
            effective_end = yesterday
            effective_start = yesterday - timedelta(days=days - 1)  # Match worker's _resolve_date_range logic: yesterday - (days-1) = days total

        date_list = [effective_start + timedelta(days=i) for i in range(days)]

        plan = {
            "mode": req.mode,
            "symbols": symbols,
            "days": days,
            "effective_range": {
                "from": effective_start.isoformat(),
                "to": effective_end.isoformat(),
            },
            "total_watchlist_items": len(symbols),
        }
        if not symbols:
            raise HTTPException(
                status_code=400, 
                detail="No symbols resolved for ingestion. Please check your Futu OpenD connection and watchlist configuration."
            )
        date_list_iso = [d.isoformat() for d in date_list]

        total_items = len(symbols) * len(date_list)  # symbol-day pairs
        total_steps = 3  # ingest, validate, commit
        date_args = ["--start-date", effective_start.isoformat(), "--end-date", effective_end.isoformat()]
        
        # Initialize detailed progress for all symbol-date pairs
        with _status_lock:
            _ingestion_status[status_id]["total_watchlist_items"] = len(symbols)
            _ingestion_status[status_id]["detailed_progress"] = [
                {"symbol": sym, "date": d.isoformat(), "status": "pending", "timestamp": None}
                for sym in symbols
                for d in date_list
            ]

        steps_log = []
        progress_snapshots = []
        step_counter = 0
        ingest_progress = None
        validate_progress = None
        commit_progress = None

        def _count_staging(conn: duckdb.DuckDBPyConnection) -> int:
            placeholders = ",".join(["?"] * len(symbols))
            date_placeholders = ",".join(["?"] * len(date_list_iso))
            res = conn.execute(
                f"""
                SELECT COUNT(DISTINCT std_symbol || ':' || file_date) AS cnt
                FROM fact_ticks_staging
                WHERE std_symbol IN ({placeholders})
                  AND file_date IN ({date_placeholders})
                """,
                symbols + date_list_iso,
            ).fetchone()
            return int(res[0]) if res and res[0] is not None else 0

        def _count_validation(conn: duckdb.DuckDBPyConnection) -> int:
            """
            Count validated items that were actually ingested in this run.
            Only count validation results for symbol-date pairs that exist in staging.
            """
            placeholders = ",".join(["?"] * len(symbols))
            date_placeholders = ",".join(["?"] * len(date_list_iso))
            res = conn.execute(
                f"""
                SELECT COUNT(DISTINCT v.std_symbol || ':' || v.file_date) AS cnt
                FROM validation_results v
                INNER JOIN fact_ticks_staging s
                  ON v.std_symbol = s.std_symbol
                  AND v.file_date = s.file_date
                WHERE v.std_symbol IN ({placeholders})
                  AND v.file_date IN ({date_placeholders})
                """,
                symbols + date_list_iso,
            ).fetchone()
            return int(res[0]) if res and res[0] is not None else 0

        def _count_trusted(conn: duckdb.DuckDBPyConnection) -> int:
            placeholders = ",".join(["?"] * len(symbols))
            date_placeholders = ",".join(["?"] * len(date_list_iso))
            res = conn.execute(
                f"""
                SELECT COUNT(DISTINCT std_symbol || ':' || date_trunc('day', ts_exchange)) AS cnt
                FROM {TICKS_TABLE}
                WHERE std_symbol IN ({placeholders})
                  AND date_trunc('day', ts_exchange) IN ({date_placeholders})
                """,
                symbols + date_list_iso,
            ).fetchone()
            return int(res[0]) if res and res[0] is not None else 0

        def snapshot_progress(phase: str):
            try:
                with get_duck(read_only=True) as duck:
                    ingested = _count_staging(duck)
                    validated = _count_validation(duck)
                    committed = _count_trusted(duck)
            except Exception as e:
                # If counting fails, still return partial info
                progress = {
                    "phase": phase,
                    "total_items": total_items,
                    "ingested_items": None,
                    "validated_items": None,
                    "committed_items": None,
                    "error": str(e),
                }
                progress_snapshots.append(progress)
                return progress

            progress = {
                "phase": phase,
                "total_items": total_items,
                "ingested_items": ingested,
                "validated_items": validated,
                "committed_items": committed,
            }
            progress_snapshots.append(progress)
            return progress

        def run_step(name: str, cmd: List[str], timeout: int = 300, progress: dict | None = None):
            """
            Run a subprocess step with timeout and real-time status updates.
            
            Note: There's a known issue where subprocesses spawned from FastAPI/uvicorn
            can crash with SIGBUS on macOS, especially when the workspace is in iCloud Drive.
            If this occurs, workers can be run manually from the command line.
            
            Args:
                name: Step name for logging
                cmd: Command to run
                timeout: Timeout in seconds (default 300 = 5 minutes)
            """
            import os
            import sys
            import re
            env = os.environ.copy()
            # Ensure PYTHONPATH is set so subprocess can find modules
            workspace_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
            if "PYTHONPATH" in env:
                env["PYTHONPATH"] = f"{workspace_path}:{env['PYTHONPATH']}"
            else:
                env["PYTHONPATH"] = workspace_path
            
            # Replace 'python3' with sys.executable to use the same Python interpreter
            cmd = list(cmd)  # Make a copy to avoid modifying the original
            if cmd[0] == "python3" or cmd[0] == "python":
                cmd[0] = sys.executable
            
            # Store original cmd for error messages (before modification)
            original_cmd_str = " ".join(cmd)
            
            # Update status
            with _status_lock:
                _ingestion_status[status_id].update({
                    "current_step": name,
                    "message": f"Running {name}...",
                })
            
            logger.info(f"[INGEST] Running step: {name}")
            logger.info(f"[INGEST] Command: {original_cmd_str}")
            
            stdout_lines = []
            stderr_lines = []
            
            try:
                # Use Popen to capture output in real-time
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                    cwd=workspace_path,
                    bufsize=1,  # Line buffered
                )
                
                # Parse output in real-time to extract symbol/date info
                def parse_output_line(line: str):
                    """Parse output line to extract symbol and date information."""
                    if not line:
                        return None, None
                    
                    # Pattern: [ingest_futu] symbol=HK.00700, from=2025-12-16 to=2025-12-16
                    symbol_match = re.search(r'symbol=([^\s,]+)', line)
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', line)
                    
                    symbol = symbol_match.group(1) if symbol_match else None
                    date_str = date_match.group(1) if date_match else None
                    
                    return symbol, date_str
                
                # Read output in real-time using threads
                import threading
                import queue
                
                def read_output(pipe, output_list, is_stderr=False):
                    """Read output from pipe and append to list."""
                    try:
                        for line in iter(pipe.readline, ''):
                            if not line:
                                break
                            line = line.strip()
                            if line:
                                output_list.append(line)
                                if is_stderr:
                                    logger.warning(f"[INGEST] {name} stderr: {line}")
                                else:
                                    logger.info(f"[INGEST] {name} output: {line}")
                                    
                                    # Parse for symbol/date info
                                    symbol, date_str = parse_output_line(line)
                                    if symbol or date_str:
                                        with _status_lock:
                                            if status_id in _ingestion_status:
                                                if symbol:
                                                    _ingestion_status[status_id]["current_symbol"] = symbol
                                                if date_str:
                                                    _ingestion_status[status_id]["current_date"] = date_str
                                                _ingestion_status[status_id]["message"] = f"{name}: {symbol or 'processing'} - {date_str or 'loading...'}"
                                                
                                                # Update detailed progress
                                                if symbol and date_str:
                                                    # Determine phase from step name
                                                    phase = "pending"
                                                    if "ingest" in name.lower():
                                                        phase = "ingest"
                                                    elif "validate" in name.lower():
                                                        phase = "validate"
                                                    elif "commit" in name.lower():
                                                        phase = "commit"
                                                    
                                                    # Update or add progress entry
                                                    detailed = _ingestion_status[status_id].get("detailed_progress", [])
                                                    found = False
                                                    for item in detailed:
                                                        if item.get("symbol") == symbol and item.get("date") == date_str:
                                                            # Update existing entry if new phase is later in pipeline
                                                            phase_order = {"pending": 0, "ingest": 1, "validate": 2, "commit": 3}
                                                            current_phase_order = phase_order.get(item.get("status", "pending"), 0)
                                                            new_phase_order = phase_order.get(phase, 0)
                                                            if new_phase_order > current_phase_order:
                                                                item["status"] = phase
                                                                item["timestamp"] = time.time()
                                                            found = True
                                                            break
                                                    
                                                    if not found:
                                                        detailed.append({
                                                            "symbol": symbol,
                                                            "date": date_str,
                                                            "status": phase,
                                                            "timestamp": time.time(),
                                                        })
                                                    
                                                    _ingestion_status[status_id]["detailed_progress"] = detailed
                    except Exception as e:
                        logger.error(f"[INGEST] Error reading {'stderr' if is_stderr else 'stdout'}: {e}")
                    finally:
                        pipe.close()
                
                # Start threads to read stdout and stderr
                stdout_thread = threading.Thread(target=read_output, args=(proc.stdout, stdout_lines, False))
                stderr_thread = threading.Thread(target=read_output, args=(proc.stderr, stderr_lines, True))
                stdout_thread.daemon = True
                stderr_thread.daemon = True
                stdout_thread.start()
                stderr_thread.start()
                
                # Wait for process to complete
                returncode = proc.wait()
                stdout = "\n".join(stdout_lines)
                stderr = "\n".join(stderr_lines)
                
                if returncode != 0:
                    error_msg = f"Step {name} failed with return code {returncode}"
                    logger.error(f"[INGEST] {error_msg}")
                    logger.error(f"[INGEST] stdout: {stdout[-1000:]}")
                    logger.error(f"[INGEST] stderr: {stderr[-1000:]}")
                    raise subprocess.CalledProcessError(returncode, cmd, stdout, stderr)
                
                steps_log.append(
                    {
                        "step": name,
                        "cmd": original_cmd_str,
                        "returncode": returncode,
                        "stdout": (stdout or "")[-4000:],
                        "stderr": (stderr or "")[-4000:],
                        "status": "ok",
                        **(progress or {}),
                    }
                )
                
                logger.info(f"[INGEST] Step {name} completed successfully")
                
            except subprocess.TimeoutExpired as e:
                logger.error(f"[INGEST] Step {name} timed out after {timeout} seconds")
                with _status_lock:
                    _ingestion_status[status_id].update({
                        "status": "error",
                        "message": f"{name} timed out after {timeout} seconds",
                    })
                steps_log.append(
                    {
                        "step": name,
                        "cmd": original_cmd_str,
                        "returncode": None,
                        "stdout": "",
                        "stderr": f"Process timed out after {timeout} seconds",
                        "status": "timeout",
                        **(progress or {}),
                    }
                )
                raise HTTPException(
                    status_code=500,
                    detail={
                        "step": name,
                        "cmd": original_cmd_str,
                        "error": f"Process timed out after {timeout} seconds",
                        "stderr": f"Process timed out after {timeout} seconds",
                        "status_id": status_id,
                    },
                )
            except subprocess.CalledProcessError as e:
                # Handle SIGBUS (returncode -10) with helpful error message
                error_detail = {
                    "step": name,
                    "cmd": original_cmd_str,
                    "returncode": e.returncode,
                    "stderr": e.stderr or "",
                    "stdout": e.stdout or "",
                    "status_id": status_id,
                }
                
                logger.error(f"[INGEST] Step {name} failed with return code {e.returncode}")
                logger.error(f"[INGEST] Command: {original_cmd_str}")
                logger.error(f"[INGEST] stdout: {(e.stdout or '')[-1000:]}")
                logger.error(f"[INGEST] stderr: {(e.stderr or '')[-1000:]}")
                
                # Check for rate limit error in stderr
                stderr_text = (e.stderr or "").lower()
                if "rate limit" in stderr_text or "futu api rate limit exceeded" in stderr_text:
                    error_detail["error"] = (
                        "Futu API rate limit exceeded. "
                        "The ingestion is trying to fetch too many symbols too quickly. "
                        "The Futu API allows 60 requests per 30 seconds. "
                        "Try reducing the number of symbols or wait a few minutes before retrying."
                    )
                    logger.error("[INGEST] Rate limit error detected")
                elif e.returncode == -10:  # SIGBUS
                    error_detail["error"] = (
                        "Subprocess crashed with SIGBUS (bus error). "
                        "This is a known issue when running workers from FastAPI/uvicorn on macOS, "
                        "especially with iCloud Drive workspaces. "
                        "Workaround: Run workers manually from the command line:\n"
                        f"  {original_cmd_str}"
                    )
                    logger.error("[INGEST] SIGBUS error detected - known macOS/iCloud Drive issue")
                else:
                    # Extract error message from stderr if available
                    if e.stderr:
                        # Try to find the actual error message
                        lines = e.stderr.split('\n')
                        for line in reversed(lines):
                            if 'Error' in line or 'Exception' in line or 'RuntimeError' in line:
                                error_detail["error"] = line.strip()
                                break
                        if "error" not in error_detail:
                            error_detail["error"] = f"Process failed with return code {e.returncode}"
                    else:
                        error_detail["error"] = f"Process failed with return code {e.returncode}"
                
                with _status_lock:
                    _ingestion_status[status_id].update({
                        "status": "error",
                        "message": f"{name} failed: {error_detail.get('error', 'Unknown error')}",
                    })
                
                steps_log.append(
                    {
                        "step": name,
                        "cmd": original_cmd_str,
                        "returncode": e.returncode,
                        "stdout": (e.stdout or "")[-4000:],
                        "stderr": (e.stderr or "")[-4000:],
                        "status": "failed",
                        **(progress or {}),
                    }
                )
                raise HTTPException(
                    status_code=500,
                    detail=error_detail,
                )
            except Exception as e:
                logger.error(f"[INGEST] Unexpected error in step {name}: {e}", exc_info=True)
                with _status_lock:
                    _ingestion_status[status_id].update({
                        "status": "error",
                        "message": f"Unexpected error in {name}: {str(e)}",
                    })
                raise HTTPException(
                    status_code=500,
                    detail={
                        "step": name,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "status_id": status_id,
                    },
                )

        # 1) ingest all
        step_counter += 1
        
        # For full_last_60d mode, process each symbol with its own date range
        if req.mode == "full_last_60d":
            # Process each symbol individually with dynamic date range
            for sym in symbols:
                # Check existing data in database to determine available date range
                try:
                    with get_duck(read_only=True) as duck:
                        # Check what dates we already have for this symbol
                        existing_dates_df = duck.execute(
                            """
                            SELECT DISTINCT date_trunc('day', ts_exchange)::DATE AS trade_date
                            FROM fact_ticks_trusted
                            WHERE std_symbol = ?
                            ORDER BY trade_date
                            """,
                            [sym],
                        ).fetch_df()
                except Exception:
                    existing_dates_df = None
                
                # Determine date range for this symbol
                if existing_dates_df is not None and not existing_dates_df.empty:
                    # We have existing data - check the range
                    dates_list = existing_dates_df["trade_date"].tolist()
                    if dates_list:
                        # Convert to date objects if needed
                        if hasattr(dates_list[0], 'date'):
                            dates_list = [d.date() if hasattr(d, 'date') else d for d in dates_list]
                        min_date = min(dates_list)
                        max_date = max(dates_list)
                        existing_days = (max_date - min_date).days + 1
                        
                        # If we have less than 60 days of existing data, try to extend the range
                        # Start from the earliest existing date and go forward to yesterday
                        # This will capture any new data since the earliest date
                        if existing_days < 60:
                            sym_start = min_date
                            sym_end = yesterday
                        else:
                            # Get last 60 days (from yesterday)
                            sym_start = yesterday - timedelta(days=59)
                            sym_end = yesterday
                    else:
                        # Empty list - try last 60 days
                        sym_start = yesterday - timedelta(days=59)
                        sym_end = yesterday
                else:
                    # No existing data - try last 60 days
                    # The worker will handle cases where less data is available
                    sym_start = yesterday - timedelta(days=59)
                    sym_end = yesterday
                
                sym_date_args = ["--start-date", sym_start.isoformat(), "--end-date", sym_end.isoformat()]
                
                run_step(
                    f"ingest_futu_{sym}",
                    ["python3", "-m", "workers.ingest_futu", "--symbols", sym, *sym_date_args],
                    progress={
                        "step_idx": step_counter,
                        "total_steps": total_steps,
                        "phase": "ingest",
                    },
                )
        else:
            # For other modes, process all symbols together
            run_step(
                "ingest_futu",
                ["python3", "-m", "workers.ingest_futu", "--symbols", *symbols, *date_args],
                progress={
                    "step_idx": step_counter,
                    "total_steps": total_steps,
                    "phase": "ingest",
                },
            )
        
        ingest_progress = snapshot_progress("ingest")

        # 2) validate A (vertical) all
        step_counter += 1
        run_step(
            "validate_A",
            ["python3", "-m", "workers.validate", "--run-a", "--symbols", *symbols, *date_args],
            progress={
                "step_idx": step_counter,
                "total_steps": total_steps,
                "phase": "validate",
            },
        )
        validate_progress = snapshot_progress("validate")

        # 3) validate B (horizontal, demo mode) all (not counted toward items but we run once)
        run_step(
            "validate_B_demo",
            ["python3", "-m", "workers.validate_cross", "--symbols", *symbols, *date_args, "--demo"],
        )

        # 4) Check validation_results: any failures? if yes -> DO NOT commit
        settings = get_settings()
        con = duckdb.connect(settings.duckdb_path, read_only=True)
        try:
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
                    dates=",".join(["?"] * len(date_list_iso)),
                ),
                symbols + date_list_iso,
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
                # Create partial date summary for failed validation
                date_summary = []
                try:
                    with get_duck(read_only=True) as duck:
                        for trade_date in date_list:
                            date_str = trade_date.isoformat()
                            placeholders = ",".join(["?"] * len(symbols))
                            
                            ingested_df = duck.execute(
                                f"""
                                SELECT COUNT(DISTINCT std_symbol) AS cnt
                                FROM fact_ticks_staging
                                WHERE std_symbol IN ({placeholders}) AND file_date = ?
                                """,
                                symbols + [date_str],
                            ).fetch_df()
                            ingested_count = int(ingested_df["cnt"][0]) if not ingested_df.empty and ingested_df["cnt"][0] is not None else 0
                            
                            validated_df = duck.execute(
                                f"""
                                SELECT COUNT(DISTINCT v.std_symbol) AS cnt
                                FROM validation_results v
                                INNER JOIN fact_ticks_staging s
                                  ON v.std_symbol = s.std_symbol
                                  AND v.file_date = s.file_date
                                WHERE v.std_symbol IN ({placeholders}) AND v.file_date = ?
                                """,
                                symbols + [date_str],
                            ).fetch_df()
                            validated_count = int(validated_df["cnt"][0]) if not validated_df.empty and validated_df["cnt"][0] is not None else 0
                            
                            date_failures = [f for f in validation_summary["failures"] if f["file_date"] == date_str]
                            validation_result = "passed" if not date_failures else "failed"
                            
                            date_summary.append({
                                "date": date_str,
                                "ingested_items": ingested_count,
                                "validated_items": validated_count,
                                "committed_items": 0,
                                "validation_result": validation_result,
                                "failed_symbols": sorted(set([f["symbol"] for f in date_failures])) if date_failures else [],
                            })
                except Exception:
                    pass
                
                return {
                    "status": "validation_failed",
                    "plan": plan,
                    "steps": steps_log,
                    "validation": validation_summary,
                    "progress_snapshots": progress_snapshots,
                    "summary_progress": validate_progress or ingest_progress,
                    "date_summary": date_summary,
                }

        symbols_to_commit = [s for s in symbols if s not in failed_symbols]

        if not symbols_to_commit:
            # Create partial date summary for no symbols to commit
            date_summary = []
            try:
                with get_duck(read_only=True) as duck:
                    for trade_date in date_list:
                        date_str = trade_date.isoformat()
                        placeholders = ",".join(["?"] * len(symbols))
                        
                        ingested_df = duck.execute(
                            f"""
                            SELECT COUNT(DISTINCT std_symbol) AS cnt
                            FROM fact_ticks_staging
                            WHERE std_symbol IN ({placeholders}) AND file_date = ?
                            """,
                            symbols + [date_str],
                        ).fetch_df()
                        ingested_count = int(ingested_df["cnt"][0]) if not ingested_df.empty and ingested_df["cnt"][0] is not None else 0
                        
                        validated_df = duck.execute(
                            f"""
                            SELECT COUNT(DISTINCT std_symbol) AS cnt
                            FROM validation_results
                            WHERE std_symbol IN ({placeholders}) AND file_date = ?
                            """,
                            symbols + [date_str],
                        ).fetch_df()
                        validated_count = int(validated_df["cnt"][0]) if not validated_df.empty and validated_df["cnt"][0] is not None else 0
                        
                        date_failures = [f for f in validation_summary["failures"] if f["file_date"] == date_str]
                        validation_result = "passed" if not date_failures else "failed"
                        
                        date_summary.append({
                            "date": date_str,
                            "ingested_items": ingested_count,
                            "validated_items": validated_count,
                            "committed_items": 0,
                            "validation_result": validation_result,
                            "failed_symbols": sorted(set([f["symbol"] for f in date_failures])) if date_failures else [],
                        })
            except Exception:
                pass
            
            return {
                "status": "validation_failed",
                "plan": plan,
                "steps": steps_log,
                "validation": validation_summary,
                "message": "No symbols passed validation; nothing committed.",
                "progress_snapshots": progress_snapshots,
                "summary_progress": validate_progress or ingest_progress,
                "date_summary": date_summary,
            }

        if failed_symbols:
            plan.setdefault("skipped_symbols", sorted(failed_symbols))

        # 5) No failures -> commit staging → trusted
        step_counter += 1
        run_step(
            "commit",
            ["python3", "-m", "workers.commit", "--symbols", *symbols_to_commit, *date_args],
            progress={
                "step_idx": step_counter,
                "total_steps": total_steps,
                "phase": "commit",
            },
        )
        commit_progress = snapshot_progress("commit")

        # Create per-date summary
        date_summary = []
        try:
            if symbols and date_list:  # Only generate summary if we have symbols and dates
                with get_duck(read_only=True) as duck:
                    for trade_date in date_list:
                        date_str = trade_date.isoformat()
                        placeholders = ",".join(["?"] * len(symbols))
                        
                        # Count ingested items (symbol-date pairs in staging)
                        ingested_df = duck.execute(
                            f"""
                            SELECT COUNT(DISTINCT std_symbol) AS cnt
                            FROM fact_ticks_staging
                            WHERE std_symbol IN ({placeholders}) AND file_date = ?
                            """,
                            symbols + [date_str],
                        ).fetch_df()
                        ingested_count = int(ingested_df["cnt"][0]) if not ingested_df.empty and ingested_df["cnt"][0] is not None else 0
                        
                        # Count validated items (only those that were ingested in staging)
                        validated_df = duck.execute(
                            f"""
                            SELECT COUNT(DISTINCT v.std_symbol) AS cnt
                            FROM validation_results v
                            INNER JOIN fact_ticks_staging s
                              ON v.std_symbol = s.std_symbol
                              AND v.file_date = s.file_date
                            WHERE v.std_symbol IN ({placeholders}) AND v.file_date = ?
                            """,
                            symbols + [date_str],
                        ).fetch_df()
                        validated_count = int(validated_df["cnt"][0]) if not validated_df.empty and validated_df["cnt"][0] is not None else 0
                        
                        # Count committed items (symbol-date pairs in trusted)
                        committed_df = duck.execute(
                            f"""
                            SELECT COUNT(DISTINCT std_symbol) AS cnt
                            FROM {TICKS_TABLE}
                            WHERE std_symbol IN ({placeholders}) 
                              AND date_trunc('day', ts_exchange) = ?
                            """,
                            symbols + [date_str],
                        ).fetch_df()
                        committed_count = int(committed_df["cnt"][0]) if not committed_df.empty and committed_df["cnt"][0] is not None else 0
                        
                        # Check validation result for this date
                        date_failures = [f for f in validation_summary["failures"] if f["file_date"] == date_str]
                        validation_result = "passed" if not date_failures else "failed"
                        
                        date_summary.append({
                            "date": date_str,
                            "ingested_items": ingested_count,
                            "validated_items": validated_count,
                            "committed_items": committed_count,
                            "validation_result": validation_result,
                            "failed_symbols": sorted(set([f["symbol"] for f in date_failures])) if date_failures else [],
                        })
        except Exception as e:
            # If summary generation fails, continue without it
            import traceback
            print(f"[dashboard] Warning: Failed to generate date summary: {e}")
            traceback.print_exc()

        with _status_lock:
            _ingestion_status[status_id].update({
                "status": "success" if not failed_symbols else "partial_success",
                "message": "Ingestion completed successfully",
            })
        
        return {
            "status": "success" if not failed_symbols else "partial_success",
            "status_id": status_id,
            "plan": plan,
            "steps": steps_log,
            "validation": validation_summary,
            "progress_snapshots": progress_snapshots,
            "summary_progress": commit_progress if commit_progress else (validate_progress if validate_progress else ingest_progress),
            "date_summary": date_summary,
        }
    except HTTPException:
        # Re-raise HTTP exceptions (they're already properly formatted)
        with _status_lock:
            if status_id in _ingestion_status:
                _ingestion_status[status_id].update({
                    "status": "error",
                    "message": "HTTP error occurred",
                })
        raise
    except Exception as e:
        # Log all other exceptions with full traceback
        logger.error(f"[INGEST] Unexpected error in dashboard_ingest: {e}", exc_info=True)
        logger.error(f"[INGEST] Request: mode={req.mode}, start_date={req.start_date}, end_date={req.end_date}, symbols={req.symbols}")
        
        with _status_lock:
            if status_id in _ingestion_status:
                _ingestion_status[status_id].update({
                    "status": "error",
                    "message": f"Unexpected error: {str(e)}",
                })
        
        # Return error details
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "error_type": type(e).__name__,
                "status_id": status_id,
                "message": "An unexpected error occurred. Check server logs for details.",
            },
        )


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
    pre { background: #000; color: #0f0; padding: 10px; border-radius: 4px; max-height: 260px; overflow: auto; }
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
    </div>
  </fieldset>
  <div class="progress" aria-label="Ingestion progress">
    <div id="ingest-progress-bar" class="progress-bar"></div>
  </div>
  <div id="ingest-progress-text" class="progress-label">Idle</div>
  <div id="ingest-status" style="font-size: 12px; color: #0f0; margin-top: 4px; min-height: 16px;"></div>
  <div id="ingest-detailed-progress" style="font-size: 11px; margin-top: 8px; max-height: 300px; overflow-y: auto; background: #f5f5f5; padding: 8px; border-radius: 4px; display: none;"></div>
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

window.pollIngestStatus = function pollIngestStatus(statusId, interval = 1000) {
  if (!statusId) return null;
  
  const statusDiv = document.getElementById('ingest-status');
  let pollCount = 0;
  const maxPolls = 3600; // Stop after 1 hour (3600 seconds)
  
  const poll = async () => {
    try {
      const res = await fetch(`/api/dashboard/ingest/status?status_id=${statusId}`);
      if (!res.ok) {
        pollCount++;
        if (pollCount < maxPolls) {
          setTimeout(poll, interval);
        }
        return;
      }
      
      const statusData = await res.json();
      const status = statusData[statusId];
      
      if (status) {
        let statusText = '';
        if (status.current_symbol && status.current_date) {
          statusText = `Processing: ${status.current_symbol} on ${status.current_date}`;
        } else if (status.current_symbol) {
          statusText = `Processing: ${status.current_symbol}`;
        } else if (status.current_step) {
          statusText = `Step: ${status.current_step}`;
        } else if (status.message) {
          statusText = status.message;
        }
        
        if (statusDiv && statusText) {
          statusDiv.textContent = statusText;
          statusDiv.style.color = status.status === 'error' ? '#dc2626' : '#0f0';
        }
        
        // Display detailed progress
        const detailedDiv = document.getElementById('ingest-detailed-progress');
        if (detailedDiv && status.detailed_progress && status.detailed_progress.length > 0) {
          detailedDiv.style.display = 'block';
          
          // Group by status
          const byStatus = {
            'pending': [],
            'ingest': [],
            'validate': [],
            'commit': []
          };
          
          status.detailed_progress.forEach(item => {
            const s = item.status || 'pending';
            if (byStatus[s]) {
              byStatus[s].push(item);
            }
          });
          
          let html = '<div style="font-weight: bold; margin-bottom: 8px;">Detailed Progress:</div>';
          html += `<div style="margin-bottom: 4px;">Total Watchlist Items: <strong>${status.total_watchlist_items || 0}</strong></div>`;
          html += '<div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 4px; font-size: 10px;">';
          
          // Show items grouped by status with colors
          const statusColors = {
            'pending': '#9ca3af',
            'ingest': '#3b82f6',
            'validate': '#f59e0b',
            'commit': '#10b981'
          };
          
          const statusLabels = {
            'pending': 'Pending',
            'ingest': 'Ingested',
            'validate': 'Validated',
            'commit': 'Committed'
          };
          
          // Show committed first, then validated, then ingested, then pending
          ['commit', 'validate', 'ingest', 'pending'].forEach(phase => {
            if (byStatus[phase] && byStatus[phase].length > 0) {
              html += `<div style="margin-top: 8px;"><strong style="color: ${statusColors[phase]}">${statusLabels[phase]} (${byStatus[phase].length}):</strong></div>`;
              byStatus[phase].slice(0, 50).forEach(item => {
                html += `<div style="padding: 2px 4px; background: ${statusColors[phase]}20; border-left: 2px solid ${statusColors[phase]}; margin: 2px 0;">${item.symbol} - ${item.date}</div>`;
              });
              if (byStatus[phase].length > 50) {
                html += `<div style="color: #666; font-style: italic;">... and ${byStatus[phase].length - 50} more</div>`;
              }
            }
          });
          
          html += '</div>';
          detailedDiv.innerHTML = html;
        }
        
        // Stop polling if status is final
        if (status.status === 'success' || status.status === 'error' || status.status === 'partial_success') {
          return;
        }
      }
      
      pollCount++;
      if (pollCount < maxPolls) {
        setTimeout(poll, interval);
      }
    } catch (e) {
      console.error('[ERROR] Status poll error:', e);
      pollCount++;
      if (pollCount < maxPolls) {
        setTimeout(poll, interval);
      }
    }
  };
  
  poll();
  return poll;
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

    const log = document.getElementById('ingest-log');
    
    // First, check if items already exist
    try {
      console.log('[DEBUG] Checking for existing items...');
      if (log) log.innerHTML = '<div style="font-size: 13px; color: #666;">[Checking] Verifying existing data...</div>';
      
      const checkRes = await fetch('/api/dashboard/ingest/check', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body)
      });
      
      if (!checkRes.ok) {
        throw new Error(`Check failed: ${checkRes.status}`);
      }
      
      const checkData = await checkRes.json();
      console.log('[DEBUG] Check result:', checkData);
      
      // If items exist, show confirmation dialog
      if (checkData.has_existing && checkData.existing_items && checkData.existing_items.length > 0) {
        // Re-enable button while waiting for confirmation
        if (btnIngest) {
          btnIngest.disabled = false;
          btnIngest.textContent = originalBtnText;
        }
        
        // Build confirmation message
        let confirmMsg = `Found ${checkData.existing_count} existing item(s) in the database:\n\n`;
        const displayItems = checkData.existing_items.slice(0, 10); // Show first 10
        displayItems.forEach(item => {
          confirmMsg += `  • ${item.symbol} on ${item.date}\n`;
        });
        if (checkData.existing_items.length > 10) {
          confirmMsg += `  ... and ${checkData.existing_items.length - 10} more\n`;
        }
        confirmMsg += `\nTotal items to process: ${checkData.total_items}\n`;
        confirmMsg += `\nExisting items will be replaced. Do you want to proceed?`;
        
        const proceed = confirm(confirmMsg);
        
        if (!proceed) {
          if (log) log.innerHTML = '<div style="font-size: 13px; color: #666;">[Cancelled] Ingestion cancelled by user.</div>';
          if (typeof window.setIngestProgress === 'function') {
            window.setIngestProgress(0, 'Cancelled');
          }
          return;
        }
        
        // User confirmed, disable button again
        if (btnIngest) {
          btnIngest.disabled = true;
          btnIngest.textContent = 'Running...';
        }
      }
    } catch (checkError) {
      console.error('[ERROR] Check failed:', checkError);
      // If check fails, ask user if they want to proceed anyway
      if (btnIngest) {
        btnIngest.disabled = false;
        btnIngest.textContent = originalBtnText;
      }
      const proceed = confirm('Unable to check for existing items. Do you want to proceed with ingestion anyway?');
      if (!proceed) {
        if (log) log.innerHTML = '<div style="font-size: 13px; color: #666;">[Cancelled] Ingestion cancelled by user.</div>';
        return;
      }
      if (btnIngest) {
        btnIngest.disabled = true;
        btnIngest.textContent = 'Running...';
      }
    }

    console.log('[DEBUG] Preparing to send request to /api/dashboard/ingest');
    if (log) log.innerHTML = '<div style="font-size: 13px; color: #666;">[Running] Processing ingestion and verification...</div>';
    if (typeof window.setIngestProgress === 'function') {
      window.setIngestProgress(10, 'Starting pipeline...');
    }
    
    // Clear status display
    const statusDiv = document.getElementById('ingest-status');
    if (statusDiv) statusDiv.textContent = '';

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
        let errorMsg = `Error: HTTP ${res.status} ${res.statusText}`;
        let statusId = null;
        try {
          const errorData = JSON.parse(responseText);
          if (errorData.detail) {
            if (typeof errorData.detail === 'string') {
              errorMsg += `\n\n${errorData.detail}`;
            } else if (errorData.detail.error) {
              errorMsg += `\n\n${errorData.detail.error}`;
              if (errorData.detail.status_id) {
                statusId = errorData.detail.status_id;
              }
            }
            if (errorData.detail.status_id) {
              statusId = errorData.detail.status_id;
            }
          }
        } catch (e) {
          // If not JSON, show first 200 chars
          errorMsg += `\n\n${responseText.substring(0, 200)}`;
        }
        console.error('[ERROR] HTTP error response:', errorMsg);
        if (log) log.innerHTML = `<div style="font-size: 13px; color: #dc2626; white-space: pre-wrap;">${errorMsg}</div>`;
        if (typeof window.setIngestProgress === 'function') {
          window.setIngestProgress(0, `Error: HTTP ${res.status}`);
        }
        
        // Start polling for status even on error if status_id is available
        if (statusId && typeof window.pollIngestStatus === 'function') {
          console.log('[DEBUG] Starting status polling for error case, status_id:', statusId);
          window.pollIngestStatus(statusId, 1000);
        }
        
        // Re-enable button on error
        if (btnIngest) {
          btnIngest.disabled = false;
          btnIngest.textContent = originalBtnText;
        }
        return;
      }
      
      // Parse successful response
      try {
        data = JSON.parse(responseText);
        console.log('[DEBUG] Response data parsed successfully:', data);
        
        // Start polling for status if status_id is provided
        if (data.status_id && typeof window.pollIngestStatus === 'function') {
          window.pollIngestStatus(data.status_id, 1000);
        }
      } catch (parseError) {
        // Response is not valid JSON
        const errorMsg = `Server returned non-JSON response (status: ${res.status})\n\nResponse body:\n${responseText.substring(0, 300)}`;
        console.error('[ERROR] Failed to parse JSON:', parseError);
        console.error('[ERROR] Response text:', responseText);
        if (log) log.innerHTML = `<div style="font-size: 13px; color: #dc2626; white-space: pre-wrap;">${errorMsg}</div>`;
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
      
      // Display clean summary instead of raw JSON
      const dateSummary = data.date_summary || [];
      const plan = data.plan || {};
      const totalWatchlistItems = plan.total_watchlist_items || 0;
      
      if (dateSummary.length > 0) {
        let summaryHtml = '<div style="font-size: 13px; line-height: 1.6; background: #000; color: #0f0; padding: 10px; border-radius: 4px;">';
        summaryHtml += '<h3 style="margin-top: 0; margin-bottom: 8px; color: #0f0;">Ingestion Summary</h3>';
        
        if (totalWatchlistItems > 0) {
          summaryHtml += `<div style="margin-bottom: 12px; padding: 8px; background: #000; color: #0f0; border-left: 3px solid #3b82f6; border-radius: 4px;">`;
          summaryHtml += `<strong style="color: #0f0;">Total Watchlist Items: <span style="color: #3b82f6;">${totalWatchlistItems}</span></strong>`;
          summaryHtml += '</div>';
        }
        
        dateSummary.forEach(item => {
          const statusColor = item.validation_result === 'passed' ? '#16a34a' : '#dc2626';
          const statusIcon = item.validation_result === 'passed' ? '✓' : '✗';
          summaryHtml += `<div style="margin-bottom: 12px; padding: 8px; background: #000; color: #0f0; border-left: 3px solid ${statusColor}; border-radius: 4px;">`;
          summaryHtml += `<strong style="color: #0f0;">${item.date}</strong> - <span style="color: ${statusColor};">${statusIcon} ${item.validation_result.toUpperCase()}</span><br>`;
          summaryHtml += `&nbsp;&nbsp;<span style="color: #0f0;">Ingested: <strong>${item.ingested_items}</strong> items | `;
          summaryHtml += `Validated: <strong>${item.validated_items}</strong> items | `;
          summaryHtml += `Committed: <strong>${item.committed_items}</strong> items</span>`;
          if (item.failed_symbols && item.failed_symbols.length > 0) {
            summaryHtml += `<br>&nbsp;&nbsp;<span style="color: #dc2626;">Failed symbols: ${item.failed_symbols.join(', ')}</span>`;
          }
          summaryHtml += '</div>';
        });
        
        summaryHtml += '</div>';
        if (log) log.innerHTML = summaryHtml;
      } else {
        // Fallback to JSON if no summary available
        if (log) log.textContent = JSON.stringify(data, null, 2);
      }

    // Update progress based on completed steps
    const steps = data.steps || [];
    const snapshots = data.progress_snapshots || [];
    const summary = data.summary_progress || (snapshots.length ? snapshots[snapshots.length - 1] : null);

    let pct = 100;
    let label = 'Completed';

    if (summary && summary.total_items) {
      const totalItems = summary.total_items;
      const committed = summary.committed_items ?? 0;
      const validated = summary.validated_items ?? 0;
      const ingested = summary.ingested_items ?? 0;

      const numerator = committed || validated || ingested;
      pct = totalItems ? Math.round((numerator / totalItems) * 100) : 0;

      if (committed) {
        label = `Committed ${committed}/${totalItems}`;
      } else if (validated) {
        label = `Validated ${validated}/${totalItems}`;
      } else if (ingested) {
        label = `Ingested ${ingested}/${totalItems}`;
      }
    } else if (steps.length) {
      const progressSteps = steps.filter(s => s.step_idx && s.total_steps);
      if (progressSteps.length) {
        const okSteps = progressSteps.filter(s => s.status === 'ok');
        const maxIdx = okSteps.length ? Math.max(...okSteps.map(s => s.step_idx)) : 0;
        const total = progressSteps[0].total_steps || progressSteps.length;
        pct = total ? Math.round((maxIdx / total) * 100) : 0;
        const last = okSteps.length ? okSteps[okSteps.length - 1] : progressSteps[progressSteps.length - 1];
        const phase = last.phase || last.step || 'step';
        label = `Step ${maxIdx}/${total}: ${phase}`;
      } else {
        const completed = steps.filter(s => s.status === 'ok').length;
        pct = Math.max(35, Math.round((completed / steps.length) * 100));
        label = data.status === 'success' ? 'Completed' : data.status === 'partial_success' ? 'Partial commit' : 'Completed with validation failures';
      }
    }

    if (typeof window.setIngestProgress === 'function') {
      window.setIngestProgress(pct, label);
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
    console.log('[DEBUG] Response status:', res.status, res.statusText);
    
    // Harden response parsing: handle non-JSON responses gracefully
    let data;
    const responseText = await res.text();
    console.log('[DEBUG] Response text received (first 500 chars):', responseText.substring(0, 500));
    
    if (!res.ok) {
      // HTTP error (4xx, 5xx)
      try {
        const errorData = JSON.parse(responseText);
        throw new Error(errorData.detail || errorData.message || `HTTP ${res.status}: ${res.statusText}`);
      } catch (parseError) {
        throw new Error(`HTTP ${res.status}: ${res.statusText}. Response: ${responseText.substring(0, 200)}`);
      }
    }
    
    try {
      data = JSON.parse(responseText);
    } catch (parseError) {
      throw new Error(`Invalid JSON response: ${parseError.message}. Response: ${responseText.substring(0, 200)}`);
    }
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
