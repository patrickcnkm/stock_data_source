
# workers/validate_cross.py
# B-group cross-source validation: futu (staging or trusted) vs Longbridge minute bars.
import argparse, duckdb, pandas as pd
from datetime import date, timedelta, datetime
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

def futu_to_1m(con, symbol: str, file_date: str) -> pd.DataFrame:
    q = """
SELECT ts_exchange, price, size FROM fact_ticks_staging
             WHERE std_symbol=? AND file_date=? ORDER BY ts_exchange"""
    df = con.execute(q, [symbol, file_date]).df()
    if df.empty:
        return pd.DataFrame(columns=["bar_time", "close", "volume"])
    df['ts_exchange'] = pd.to_datetime(df['ts_exchange'])
    df['bar_time'] = df['ts_exchange'].dt.floor('min')
    g = df.groupby('bar_time', as_index=False).agg(
        close=('price','last'),
        volume=('size','sum')
    )
    return g[['bar_time', 'close', 'volume']]

def compare_bars(left: pd.DataFrame, right: pd.DataFrame):
    merged = pd.merge(left, right, on='bar_time', how='inner', suffixes=('_futu','_lb'))
    if merged.empty:
        return None
    # deviations
    mid = (merged['close_futu'] + merged['close_lb']) / 2.0
    price_dev = (abs(merged['close_futu'] - merged['close_lb']) / mid).quantile(0.95) * 100.0  # in %
    vol_dev = (abs(merged['volume_futu'] - merged['volume_lb']) / merged[['volume_futu','volume_lb']].max(axis=1)).quantile(0.95) * 100.0
    res = {
        'price_dev_p95_pct': float(price_dev),
        'vol_dev_p95_pct': float(vol_dev),
        'coverage_ratio': float(len(merged) / max(len(left), 1)),
    }
    return res

def _build_date_list(days: int, start_date: date | None, end_date: date | None) -> list[str]:
    today = date.today()
    yesterday = today - timedelta(days=1)
    if start_date and end_date:
        if end_date > yesterday:
            end_date = yesterday
        if start_date > end_date:
            raise ValueError("start_date must be <= end_date")
        count = (end_date - start_date).days + 1
        return [(start_date + timedelta(days=i)).isoformat() for i in range(count)]
    if days <= 0:
        raise ValueError("days must be > 0")
    return [(yesterday - timedelta(days=i)).isoformat() for i in range(days)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--symbols', nargs='+', required=True)
    ap.add_argument('--days', type=int, default=2)
    ap.add_argument('--start-date', type=str, default=None)
    ap.add_argument('--end-date', type=str, default=None)
    ap.add_argument('--demo', action='store_true', help='Use futu 1m vs futu 1m (self-check) as placeholder')
    args = ap.parse_args()

    # Create a separate connection for workers (write mode)
    # This avoids conflicts with the API server which uses read-only connections
    settings = get_settings()
    con = duckdb.connect(settings.duckdb_path, read_only=False)
    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    dates = _build_date_list(args.days, start_date, end_date)

    for s in args.symbols:
        # Normalize symbol before processing
        normalized_s = _normalize_symbol(s)
        for d in dates:
            futu_1m = futu_to_1m(con, normalized_s, d)
            if futu_1m.empty:
                print(f"[B-VALID] {normalized_s} {d} futu data empty, skip")
                continue
            if args.demo:
                lb_1m = futu_1m.copy()  # perfect match for demo
            else:
                from app.clients.longbridge_adapter import LongbridgeAdapter
                st = get_settings()
                raise SystemExit("Please implement LongbridgeAdapter.fetch_1m_bars and rerun with --demo off.")
            if lb_1m.empty:
                print(f"[B-VALID] {normalized_s} {d} comparison dataset empty, skip")
                continue
            lb_df = lb_1m[['bar_time','close','volume']].copy()

            res = compare_bars(
                futu_1m[['bar_time','close','volume']],
                lb_df
            )
            if not res:
                print(f"[B-VALID] {normalized_s} {d} no overlap bars")
                continue
            # thresholds from design: price 0.05%, volume 3%, coverage >= 0.98
            price_ok = res['price_dev_p95_pct'] <= 0.05
            vol_ok = res['vol_dev_p95_pct'] <= 3.0
            cov_ok = res['coverage_ratio'] >= 0.98

            con.execute("""
INSERT INTO validation_results
(run_id, std_symbol, file_date, rule_name, metric, threshold, passed, details)
VALUES (UUID(), ?, ?, 'B_cross_p95_price_pct', ?, 0.05, ?, NULL)
""", [normalized_s, d, res['price_dev_p95_pct'], price_ok])
            con.execute("""
INSERT INTO validation_results
VALUES (UUID(), ?, ?, 'B_cross_p95_vol_pct', ?, 3.0, ?, NULL)
""", [normalized_s, d, res['vol_dev_p95_pct'], vol_ok])
            con.execute("""
INSERT INTO validation_results
VALUES (UUID(), ?, ?, 'B_cross_coverage', ?, 0.98, ?, NULL)
""", [normalized_s, d, res['coverage_ratio'], cov_ok])

            print(f"[B-VALID] {normalized_s} {d} price_p95={res['price_dev_p95_pct']:.4f}% vol_p95={res['vol_dev_p95_pct']:.3f}% cov={res['coverage_ratio']:.3f} -> passed={price_ok and vol_ok and cov_ok}")

if __name__ == '__main__':
    main()
