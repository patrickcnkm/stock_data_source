# Quant Platform Minimal (MVP)

This is a minimal runnable skeleton of the Quant Data Platform:
- FastAPI + Redis Streams + DuckDB
- Prometheus `/metrics` exporter
- 2-day data ingest (mock CSV), A-group validation, commit to Trusted
- Simple WebSocket broadcast of quotes
- Basic endpoints for quotes, trusted bars, and validation results

## Prerequisites

- Python 3.11+
- Redis (running locally on port 6379)
- DuckDB (installed via requirements.txt)

## Quickstart

```bash
# 1. Create virtual environment and install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Create required directories
mkdir -p data/lake raw_demo

# 3. Initialize database schema
python scripts/init_duckdb.py

# 4. Generate demo data (optional, for testing)
python scripts/seed_demo.py         # creates 2-day mock ticks CSVs

# 5. Start the FastAPI server
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Then visit:
- API docs: http://localhost:8000/docs
- Health check: http://localhost:8000/api/health
- Prometheus: http://localhost:8000/metrics

## Mock Ingest (2-day demo)

Run the complete pipeline:
```bash
# Make sure PYTHONPATH is set to project root
export PYTHONPATH="."

# 1. Ingest demo data from CSVs
python workers/ingest_futu.py --demo --symbols 00700.HK AAPL --days 2

# 2. Validate A-group rules
python workers/validate.py --run-a --symbols 00700.HK AAPL --days 2

# 3. Commit to trusted tables
python workers/commit.py --symbols 00700.HK AAPL --days 2
```

This simulates ingest from CSVs under `raw_demo/`, validates A-group rules (coverage and monotonic timestamps), and commits to `fact_ticks_trusted` with 1-minute aggregation.

## Architecture Notes

### Database Connection Management

The application uses context managers for DuckDB connections to avoid locking conflicts:
- **API server**: Opens read-only connections per request (no locking)
- **Worker scripts**: Create independent write connections when needed

This allows the API server to run concurrently with worker scripts without database lock conflicts.

### Data Flow

1. **Staging**: Raw tick data → `fact_ticks_staging`
2. **Validation**: A-group rules check data quality → `validation_results`
3. **Trusted**: Validated data → `fact_ticks_trusted` + 1-minute bars → `fact_bars_1m_trusted`

### API Endpoints

- `GET /api/health` - Health check
- `GET /api/trusted/bars?symbol=SYMBOL&start=YYYY-MM-DD HH:MM:SS&end=YYYY-MM-DD HH:MM:SS` - Query 1-minute bars
- `GET /api/validation/results?date=YYYY-MM-DD` - Get validation results for a date
- `GET /metrics` - Prometheus metrics
- `WS /ws/quotes` - WebSocket quotes stream

## Development Notes

- Replace demo ingest with real Futu API pulling (see `workers/ingest_futu.py` TODOs).
- Redis Streams are used for `bus:ticks:*` demo pushes.
- Subscription rotation scheduler is left as a TODO stub under `workers/scheduler.py`.
- The schema has been updated to remove invalid DuckDB compression settings for compatibility.