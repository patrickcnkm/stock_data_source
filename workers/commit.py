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
    # Normalize symbol for trusted table
    normalized_symbol = _normalize_symbol(symbol)
    
    # Query staging with original symbol format (staging may have unnormalized symbols)
    # Try both original and normalized formats to handle both cases
    staging_data = con.execute("""
SELECT std_symbol, ts_exchange, price, size, trade_id, cond, file_date
FROM fact_ticks_staging 
WHERE file_date=? AND (std_symbol=? OR std_symbol=?)
""", [file_date, symbol, normalized_symbol]).fetchdf()
    
    if staging_data.empty:
        return  # No data in staging for this symbol/date
    
    # Normalize all symbols in the result to ensure consistency
    staging_data['std_symbol'] = staging_data['std_symbol'].apply(_normalize_symbol)
    
    # Delete existing data for this symbol/date combination first (to allow overwrite)
    con.execute("""
DELETE FROM fact_ticks_trusted WHERE std_symbol=? AND file_date=?
""", [normalized_symbol, file_date])
    
    # Insert into trusted with normalized symbols
    con.register("staging_tmp", staging_data)
    con.execute("""
INSERT INTO fact_ticks_trusted
SELECT std_symbol, ts_exchange, price, size, trade_id, cond, file_date
FROM staging_tmp
""")

    # 1m aggregation - delete existing bars for this symbol/date first
    con.execute("""
DELETE FROM fact_bars_1m_trusted WHERE std_symbol=? AND file_date=?
""", [normalized_symbol, file_date])
    
    df = con.execute("""
SELECT ts_exchange, price, size FROM fact_ticks_trusted
WHERE std_symbol=? AND file_date=? ORDER BY ts_exchange
""", [normalized_symbol, file_date]).df()
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
INSERT INTO fact_bars_1m_trusted
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
    
    # Group by date: for each date, delete ALL existing data first, then commit all symbols
    # This ensures "full" ingestion overwrites "partial" ingestion
    for d in dates:
        # Delete all existing data for this date (to allow full overwrite)
        con.execute("""
DELETE FROM fact_ticks_trusted WHERE file_date=?
""", [d])
        con.execute("""
DELETE FROM fact_bars_1m_trusted WHERE file_date=?
""", [d])
        
        # Now commit all symbols for this date
        for s in args.symbols:
            # Normalize symbol before processing
            normalized_s = _normalize_symbol(s)
            commit_partition(con, normalized_s, d)
            print(f"[COMMIT] {normalized_s} {d} committed to trusted + 1m")

if __name__ == "__main__":
    main()