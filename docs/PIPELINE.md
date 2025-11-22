Data Pipeline and State Machine (MVP)
======================================

## Data States

- **raw_demo**: Optional CSV seed data for offline demos
- **raw**: Canonical ticks pulled from Futu (`fact_ticks_raw`)
- **staging**: Candidate ticks awaiting validation (`fact_ticks_staging`)
- **trusted**: Trusted ticks (`fact_ticks_trusted`)
- **bars_1m_trusted**: Trusted 1-minute aggregated bars

## Pipeline Steps

### 1. Ingest → Raw
```bash
python workers/ingest_futu.py --symbols 00700.HK AAPL --days 2
```
- Connects to Futu OpenD via `FUTU_OPEND_IP/FUTU_OPEND_QUOTE_PORT`
- Looks up latest `fact_ticks_raw` date per symbol and incrementally backfills day by day
- Writes raw ticks (`symbol`, `ts_ex`, `price`, `size`, `bid`, `ask`) into DuckDB; duplicate days are replaced atomically
- Pass `--days N` to cap lookback window; CLI automatically skips already ingested dates

### 2. Optional Demo Seed
```bash
python scripts/seed_demo.py && python workers/ingest_futu.py --symbols 00700.HK --days 2 --demo
```
- Use when OpenD is unavailable; uploads mock CSVs instead (legacy path)

### 3. Validate A-Group Rules
```bash
python workers/validate.py --run-a --symbols 00700.HK AAPL --days 2
```
- **A_coverage_rows**: Checks that data exists for the date (count > 0)
- **A_monotonic_ts**: Validates timestamps are monotonically increasing
- Results written to `validation_results` table

### 4. Commit → Trusted
```bash
python workers/commit.py --symbols 00700.HK AAPL --days 2
```
- Moves validated data from `fact_ticks_staging` to `fact_ticks_trusted` (`INSERT OR IGNORE`)
- Aggregates to 1-minute bars and writes to `fact_bars_1m_trusted` (`INSERT OR REPLACE`)

### 5. API Access
- Trusted bars are queryable via `/api/trusted/bars`
- Validation results available via `/api/validation/results`
- `/api/status` + `/dashboard` reflect live Redis-backed state (subscription queues, etc.)

### 6. Streams + Strategy Runner (Optional)
```bash
python workers/streams_gateway.py --symbols 00700.HK AAPL --days 2 --sleep-ms 2
python workers/strategy_runner.py --symbol 00700.HK --group strat_vwap_break --consumer worker-1
```
- Streams gateway replays staging ticks into Redis (`bus:ticks:<symbol>`)
- Strategy runner consumes via consumer groups, tracks VWAP break strategy, outputs signals (`bus:signals:vwap_break:<symbol>`) and Prometheus metrics

### 7. Backtesting (Offline)
```bash
python workers/backtest.py --symbol 00700.HK --start 2025-11-19 --end 2025-11-21
```
- Reads trusted ticks from DuckDB and replays the same VWAP logic
- Reports trade count, round trips, P&L, and win rate for quick benchmarking

## Connection Management

The pipeline uses context managers for DuckDB connections:
- **API Server**: Opens read-only connections per request (no locking)
- **Workers**: Create independent write connections when executing tasks

This allows concurrent execution without database lock conflicts.

## Error Handling

- Validation failures are logged to `validation_results` with `passed=false`
- Failed validations do not prevent data from being ingested into staging
- Only validated data moves to trusted tables

## Future Enhancements

- B-group validation (cross-source comparison with Longbridge once adapter is implemented)
- Quarantine table for failed validations
- Retry mechanism for failed commits
- Streaming validation in real-time
- Expand dashboard to show Prometheus metrics & Redis lag in one place
