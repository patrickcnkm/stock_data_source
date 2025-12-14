# Quant Platform Minimal (MVP)

This is a minimal runnable skeleton of the Quant Data Platform:
- FastAPI + Redis Streams + DuckDB
- Prometheus `/metrics` exporter
- Web dashboard for data ingestion, monitoring, and management
- Futu API integration with rate limiting and quota tracking
- Watchlist-based stock universe management
- 2-day data ingest (mock CSV), A-group validation, commit to Trusted
- Simple WebSocket broadcast of quotes
- Basic endpoints for quotes, trusted bars, and validation results

## Prerequisites

- Python 3.11+
- Redis (running locally on port 6379, optional)
- DuckDB (installed via requirements.txt)
- Futu OpenD application (for real-time data ingestion, optional - can use demo mode)

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
- **Dashboard**: http://localhost:8000/dashboard - Interactive web UI for ingestion and monitoring
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
- **Worker scripts**: Create independent write connections when needed, with retry logic for lock conflicts

This allows the API server to run concurrently with worker scripts without database lock conflicts. Worker scripts automatically retry with exponential backoff if they encounter database locks.

### Rate Limiting

The application includes built-in rate limiting for Futu API calls:
- **30-second window**: Maximum 60 requests per 30 seconds (Futu API limit)
- **Total quota**: Maximum 300 requests (configurable, auto-resets after 24 hours)
- **Automatic throttling**: Minimum 0.6 seconds between requests

Rate limiting is enforced in:
- `workers/ingest_futu.py` - Before each API call
- `app/universe.py` - Before fetching stock universe or watchlist

Quota status can be monitored via the dashboard or `GET /api/dashboard/quota` endpoint.

### Watchlist Integration

The platform integrates with Futu OpenD watchlist (favorite stocks) to automatically determine the stock universe:
- **Watchlist mode**: Uses `load_hk_stocks_from_watchlist()` to fetch HK stocks from your Futu OpenD watchlist
- **Automatic filtering**: Only includes HK stocks with stock type "STOCK"
- **Fallback**: Can use cached universe or explicit symbol lists if watchlist is unavailable

### Data Flow

1. **Ingestion**: Futu API → `fact_ticks_staging` (raw tick data)
2. **Validation**: A-group rules check data quality → `validation_results`
3. **Commit**: Validated data → `fact_ticks_trusted` + 1-minute bars → `fact_bars_1m_trusted`

### Web Dashboard

The dashboard (`/dashboard`) provides:

1. **Data Ingestion & Verification**:
   - **Partial mode**: Ingest specific stocks with optional date range
   - **Full mode (last 60 days)**: Ingest all HK stocks from Futu watchlist, last 60 days
   - **Full mode (custom range)**: Ingest all HK stocks from Futu watchlist with custom date range
   - Progress tracking and real-time status updates
   - Ingestion statistics per trade date

2. **Data Overview**:
   - Coverage summary showing date range and total symbols
   - Daily coverage table with symbol counts
   - Quota status monitoring (30-second window and total quota)

3. **Data Operations**:
   - Delete data from trusted tables by symbols and/or date range

4. **Quota Management**:
   - Monitor Futu API quota usage (60 requests per 30 seconds, 300 total)
   - Reset application quota counter
   - Visual indicators for quota levels

### API Endpoints

**Core Endpoints:**
- `GET /api/health` - Health check
- `GET /api/trusted/bars?symbol=SYMBOL&start=YYYY-MM-DD HH:MM:SS&end=YYYY-MM-DD HH:MM:SS` - Query 1-minute bars
- `GET /api/validation/results?date=YYYY-MM-DD` - Get validation results for a date
- `GET /metrics` - Prometheus metrics
- `WS /ws/quotes` - WebSocket quotes stream

**Dashboard Endpoints:**
- `GET /dashboard` - Web dashboard UI
- `POST /api/dashboard/ingest` - Trigger ingestion pipeline
- `GET /api/dashboard/ingest/stats?trade_date=YYYY-MM-DD` - Get ingestion statistics
- `GET /api/dashboard/coverage/summary` - Get coverage summary
- `GET /api/dashboard/coverage/daily` - Get daily coverage data
- `GET /api/dashboard/quota` - Get Futu API quota status
- `POST /api/dashboard/quota/reset` - Reset quota counter
- `POST /api/dashboard/delete` - Delete data from trusted tables

## Key Features

### Rate Limiting & Quota Management
- Automatic rate limiting for all Futu API calls
- Quota tracking with visual dashboard indicators
- Auto-reset after 24 hours (matching Futu's typical reset cycle)
- Manual quota reset capability

### Watchlist-Based Stock Universe
- Automatically loads HK stocks from Futu OpenD watchlist
- Supports both watchlist-based and explicit symbol list modes
- Normalizes symbols to consistent format (HK.00700)

### Database Lock Handling
- Automatic retry logic with exponential backoff for database locks
- Clear error messages with troubleshooting guidance
- Safe concurrent access between API server and worker scripts

### Error Handling
- Comprehensive error handling in dashboard JavaScript
- Graceful handling of HTTP errors and non-JSON responses
- Detailed error messages displayed in UI
- Proper quota error propagation (prevents masking of rate limit errors)

## Development Notes

- **Futu API Integration**: Real Futu API pulling is implemented in `workers/ingest_futu.py` with rate limiting.
- **Redis Streams**: Used for `bus:ticks:*` demo pushes (optional).
- **Subscription Rotation**: Scheduler stub available under `workers/scheduler.py`.
- **Database Schema**: Updated to remove invalid DuckDB compression settings for compatibility.
- **iCloud Drive Compatibility**: Lazy imports used for Futu API modules to avoid permission issues on macOS iCloud Drive.