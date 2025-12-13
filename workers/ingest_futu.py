# workers/ingest_futu.py
"""从 Futu 自动抓取历史数据并写入 DuckDB，支持「向前 N 天」增量抓取。

使用方式示例：
    python -m workers.ingest_futu --symbols 00700.HK AAPL --days 2

设计要点：
- 使用 app.settings 中的 futu_opend_ip / futu_opend_quote_port
- 从 DuckDB 里查出该 symbol 已有的最大交易日期，向后补齐
- 本脚本写入 raw 层：fact_ticks_raw(symbol, ts_ex, price, size, bid, ask)
"""

from __future__ import annotations
import argparse
import time
from datetime import date, datetime, timedelta
from typing import List

import duckdb
import pandas as pd
from futu import OpenQuoteContext, RET_OK, KLType

from app.settings import get_settings
from app.futu_rate_limit import rate_limit

settings = get_settings()


def _connect_duck(max_retries: int = 5, retry_delay: float = 1.0):
    """
    Connect to DuckDB with retry logic for lock conflicts.
    
    DuckDB doesn't support concurrent write connections, so if another
    process is holding a lock, we retry with exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            return duckdb.connect(settings.duckdb_path)
        except Exception as e:
            if "lock" in str(e).lower() or "conflicting" in str(e).lower():
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    print(f"[ingest_futu] Database lock detected, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})...")
                    time.sleep(wait_time)
                    continue
                else:
                    error_msg = (
                        f"Could not acquire database lock after {max_retries} attempts.\n"
                        "This usually means another process is using the database.\n"
                        "Possible solutions:\n"
                        "  1. Wait a few seconds and try again (another ingestion may be running)\n"
                        "  2. Check for other Python processes using the database:\n"
                        "     ps aux | grep -i python | grep -i duck\n"
                        "  3. If a process is stuck, you may need to kill it:\n"
                        "     kill <PID> (use the PID from the error message above)\n"
                        f"  4. As a last resort, delete the lock file (if it exists):\n"
                        f"     rm -f {settings.duckdb_path}.wal.lock"
                    )
                    raise RuntimeError(error_msg) from e
            raise
    raise RuntimeError("Failed to connect to database")


def _ensure_table(conn: duckdb.DuckDBPyConnection):
    # Ensure staging table exists (matches schema.sql)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_ticks_staging (
            src         VARCHAR,
            std_symbol  VARCHAR,
            ts_exchange TIMESTAMP,
            ts_local    TIMESTAMP,
            price       DOUBLE,
            size        BIGINT,
            trade_id    VARCHAR,
            cond        VARCHAR,
            file_date   DATE,
            PRIMARY KEY (src, std_symbol, ts_exchange, trade_id)
        );
        """
    )


def _latest_trade_date(conn: duckdb.DuckDBPyConnection, symbol: str) -> date | None:
    """Returns the latest trade date (as date object) for the symbol, or None if no data exists."""
    res = conn.execute(
        "SELECT max(date_trunc('day', ts_exchange)) FROM fact_ticks_staging WHERE std_symbol = ?",
        [symbol],
    ).fetchone()
    if not res or res[0] is None:
        return None
    # date_trunc returns a date object, not datetime
    result = res[0]
    if isinstance(result, datetime):
        return result.date()
    return result


def _date_range(start: datetime, end: datetime):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def _last_err(ctx: OpenQuoteContext) -> str:
    getter = getattr(ctx, "get_last_error", None) or getattr(ctx, "get_last_err", None)
    if callable(getter):
        err = getter()
        return err if isinstance(err, str) else str(err)
    return "unknown"


def _fetch_with_request_history(ctx: OpenQuoteContext, symbol: str, start: str, end: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    page_req_key = None
    while True:
        rate_limit()  # Rate limit before each API call
        ret, df, page_req_key = ctx.request_history_kline(
            code=symbol,
            start=start,
            end=end,
            ktype=KLType.K_1M,
            max_count=1000,
            autype="qfq",
            page_req_key=page_req_key,
        )
        if ret != RET_OK:
            err = df if isinstance(df, str) else _last_err(ctx)
            print(f"[ingest_futu] request_history_kline failed: code={symbol}, ret={ret}, err={err}")
            return pd.DataFrame()
        if df is not None and not df.empty:
            frames.append(df.copy())
        if not page_req_key:
            break
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_with_legacy(ctx: OpenQuoteContext, symbol: str, start: str, end: str) -> pd.DataFrame:
    rate_limit()  # Rate limit before each API call
    ret, df = ctx.get_history_kline(
        code=symbol,
        start=start,
        end=end,
        ktype=KLType.K_1M,
        max_count=None,
        autype="qfq",
    )
    if ret != RET_OK or df is None or df.empty:
        print(f"[ingest_futu] get_history_kline failed: code={symbol}, ret={ret}, err={_last_err(ctx)}")
        return pd.DataFrame()
    return df


def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol to Futu format: HK.00700, US.AAPL, etc."""
    symbol = symbol.strip().upper()
    # Already in correct format (HK.00700, US.AAPL)
    if "." in symbol and (symbol.startswith("HK.") or symbol.startswith("US.") or 
                          symbol.startswith("SZ.") or symbol.startswith("SH.")):
        return symbol
    # Format like 00700.HK -> HK.00700
    if symbol.endswith(".HK"):
        code = symbol[:-3]  # Remove .HK
        return f"HK.{code}"
    # Format like 00700 -> HK.00700 (assume HK if no market indicator)
    if symbol.isdigit() or (symbol.replace(".", "").isdigit()):
        return f"HK.{symbol}"
    # Default: assume US stock
    return f"US.{symbol}"


def _fetch_one_day_ticks(ctx: OpenQuoteContext, symbol: str, day: datetime) -> pd.DataFrame:
    """根据 SDK 能力在 request_history_kline / get_history_kline 之间切换。"""
    start = day.strftime("%Y-%m-%d")
    end = (day + timedelta(days=1)).strftime("%Y-%m-%d")
    futu_symbol = _normalize_symbol(symbol)

    if hasattr(ctx, "request_history_kline"):
        df_all = _fetch_with_request_history(ctx, futu_symbol, start, end)
    elif hasattr(ctx, "get_history_kline"):
        df_all = _fetch_with_legacy(ctx, futu_symbol, f"{start} 00:00:00", f"{end} 00:00:00")
    else:
        raise RuntimeError("OpenQuoteContext missing history kline APIs")

    if df_all.empty:
        return pd.DataFrame()

    # Transform to staging table format
    ts_exchange = pd.to_datetime(df_all["time_key"])
    day_start = pd.Timestamp(day.date())
    day_end = day_start + timedelta(days=1)
    mask = (ts_exchange >= day_start) & (ts_exchange < day_end)
    if not mask.any():
        return pd.DataFrame()
    df_all = df_all.loc[mask].copy().reset_index(drop=True)
    ts_exchange = ts_exchange[mask].reset_index(drop=True)

    out = pd.DataFrame({
        "src": ["futu"] * len(df_all),
        "std_symbol": [futu_symbol] * len(df_all),  # Store normalized symbol (HK.00700 format)
        "ts_exchange": ts_exchange,
        "ts_local": ts_exchange,  # Use same timestamp for now
        "price": df_all["close"].astype(float),
        "size": df_all["volume"].fillna(0).astype("int64"),
        "trade_id": [f"{ts}_{idx}" for ts, idx in zip(ts_exchange.astype(str), df_all.index)],
        "cond": ["N"] * len(df_all),  # Normal trade
        "file_date": [day.date()] * len(df_all),
    })
    return out


def _resolve_date_range(days: int, start_date: date | None, end_date: date | None) -> tuple[date, date]:
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    if start_date and end_date:
        if end_date > yesterday:
            end_date = yesterday
        if start_date > end_date:
            raise ValueError("start_date must be <= end_date")
        return start_date, end_date
    target_end = yesterday
    if days <= 0:
        raise ValueError("days must be > 0")
    target_start = yesterday - timedelta(days=days - 1)
    return target_start, target_end


def ingest_symbols(symbols: List[str], days: int, start_date: date | None = None, end_date: date | None = None):
    conn = None
    ctx = None
    try:
        conn = _connect_duck()
        _ensure_table(conn)

        ctx = OpenQuoteContext(
            host=settings.futu_opend_ip,
            port=settings.futu_opend_quote_port,
        )
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        target_start, target_end = _resolve_date_range(days, start_date, end_date)
        explicit_range = start_date is not None and end_date is not None

        # Normalize all symbols to Futu format for consistency
        normalized_symbols = [_normalize_symbol(s) for s in symbols]

        for sym in normalized_symbols:
            latest = _latest_trade_date(conn, sym)
            start_for_sym = target_start
            if not explicit_range and latest is not None and latest >= target_start:
                candidate = latest + timedelta(days=1)
                if candidate > target_end:
                    print(f"[ingest_futu] symbol={sym} already ingested through {latest}, skipping")
                    continue
                start_for_sym = candidate

            print(f"[ingest_futu] symbol={sym}, from={start_for_sym} to={target_end}")

            cur_day = datetime.combine(start_for_sym, datetime.min.time())
            end_day = datetime.combine(target_end, datetime.min.time())

            while cur_day <= end_day:
                df = _fetch_one_day_ticks(ctx, sym, cur_day)
                if df.empty:
                    print(f"  - {cur_day.date()} no data")
                else:
                    # Delete existing data for this symbol/date combination
                    conn.execute(
                        "DELETE FROM fact_ticks_staging WHERE std_symbol=? AND file_date=?",
                        [sym, cur_day.date()],
                    )
                    conn.register("ticks_tmp", df)
                    conn.execute("""
                        INSERT INTO fact_ticks_staging (src, std_symbol, ts_exchange, ts_local, price, size, trade_id, cond, file_date)
                        SELECT src, std_symbol, ts_exchange, ts_local, price, size, trade_id, cond, file_date
                        FROM ticks_tmp
                    """)
                    print(f"  + {cur_day.date()} rows={len(df)}")

                cur_day += timedelta(days=1)

    finally:
        # Ensure connections are closed even if errors occur
        if ctx is not None:
            try:
                ctx.close()
            except Exception as e:
                print(f"[ingest_futu] Warning: Error closing Futu context: {e}")
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                print(f"[ingest_futu] Warning: Error closing database connection: {e}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", required=True)
    ap.add_argument("--days", type=int, default=2,
                    help="向前回溯的天数（含今天），默认 2 天")
    ap.add_argument("--start-date", type=str, default=None,
                    help="起始交易日 (YYYY-MM-DD)，与 --end-date 搭配使用")
    ap.add_argument("--end-date", type=str, default=None,
                    help="结束交易日 (YYYY-MM-DD)")
    args = ap.parse_args()

    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    end_date = date.fromisoformat(args.end_date) if args.end_date else None

    ingest_symbols(args.symbols, args.days, start_date=start_date, end_date=end_date)


if __name__ == "__main__":
    main()
