import argparse, duckdb, pandas as pd, uuid
from datetime import datetime
from app.settings import get_settings

def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol to HK.XXXXX format consistently."""
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

def validate_longitudinal(con, symbol: str, file_date: str, coverage_min=0.99, dedup_max=0.001):
    # Coverage: ensure rows exist across market session (approx via count)
    q = """
SELECT COUNT(*) FROM fact_ticks_staging WHERE std_symbol=? AND file_date=?"""
    cnt = con.execute(q, [symbol, file_date]).fetchone()[0]
    passed = cnt > 0
    con.execute("""
INSERT INTO validation_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", [
        str(uuid.uuid4()), symbol, file_date, "A_coverage_rows", float(cnt), 1.0, passed, None
    ])
    return passed

def aggregate_1m(df: pd.DataFrame):
    df['bar_time'] = pd.to_datetime(df['ts_exchange']).dt.floor('min')
    g = df.groupby('bar_time', as_index=False).agg(
        open=('price','first'),
        high=('price','max'),
        low=('price','min'),
        close=('price','last'),
        volume=('size','sum'),
        vwap=('price','mean'),
        trades=('price','count')
    )
    return g

def run_A(con, symbol: str, file_date: str):
    df = con.execute("""
SELECT ts_exchange, price, size FROM fact_ticks_staging
WHERE std_symbol=? AND file_date=? ORDER BY ts_exchange
""", [symbol, file_date]).df()
    if df.empty:
        return False
    # simple monotonic check (allow equals)
    mono = (df['ts_exchange'].diff().fillna(pd.Timedelta(0)) >= pd.Timedelta(0)).all()
    # Convert numpy bool to Python bool for DuckDB compatibility
    mono_bool = bool(mono)
    con.execute("""
INSERT INTO validation_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", [
        str(uuid.uuid4()), symbol, file_date, "A_monotonic_ts", 1.0 if mono_bool else 0.0, 1.0, mono_bool, None
    ])
    return mono

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-a", action="store_true")
    ap.add_argument("--symbols", nargs="+", required=True)
    ap.add_argument("--days", type=int, default=2)
    args = ap.parse_args()

    from datetime import timedelta, date
    end = date.today()
    dates = [ (end - timedelta(days=i+1)).isoformat() for i in range(args.days) ]

    # Create a separate connection for workers (write mode)
    # This avoids conflicts with the API server which uses read-only connections
    settings = get_settings()
    con = duckdb.connect(settings.duckdb_path, read_only=False)

    for s in args.symbols:
        # Normalize symbol before processing
        normalized_s = _normalize_symbol(s)
        for d in dates:
            ok1 = validate_longitudinal(con, normalized_s, d)
            ok2 = run_A(con, normalized_s, d)
            print(f"[A-VALID] {normalized_s} {d} coverage={ok1} monotonic={ok2}")

if __name__ == "__main__":
    main()