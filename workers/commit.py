import argparse, duckdb, pandas as pd
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

def commit_partition(con, symbol: str, file_date: str):
    # Normalize symbol before committing
    symbol = _normalize_symbol(symbol)
    # move from staging to trusted
    con.execute("""
INSERT OR IGNORE INTO fact_ticks_trusted
SELECT std_symbol, ts_exchange, price, size, trade_id, cond, file_date
FROM fact_ticks_staging WHERE std_symbol=? AND file_date=?
""", [symbol, file_date])

    # 1m aggregation
    df = con.execute("""
SELECT ts_exchange, price, size FROM fact_ticks_trusted
WHERE std_symbol=? AND file_date=? ORDER BY ts_exchange
""", [symbol, file_date]).df()
    if df.empty:
        return
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
    g['std_symbol'] = symbol
    g['file_date'] = pd.to_datetime(file_date).date()
    con.execute("""
INSERT OR REPLACE INTO fact_bars_1m_trusted
SELECT std_symbol, bar_time, open, high, low, close, volume, vwap, trades, file_date FROM g
""")

def main():
    ap = argparse.ArgumentParser()
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
            commit_partition(con, normalized_s, d)
            print(f"[COMMIT] {normalized_s} {d} committed to trusted + 1m")

if __name__ == "__main__":
    main()