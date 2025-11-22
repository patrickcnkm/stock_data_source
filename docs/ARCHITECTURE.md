Architecture Overview (Minimal MVP)
====================================

## Goal

Build a minimal runnable skeleton with 2-day data window completing the full pipeline:
**Ingest → Validate A → Commit → 1m Aggregation → API/metrics**

**Tech Stack**: FastAPI + Redis Streams (skeleton) + DuckDB + Prometheus Exporter

## Directory Structure

```
quant-platform-minimal/
  app/                       # Application & Infrastructure
    main.py                 # FastAPI entry (/metrics, /api/*, /ws/*)
    settings.py             # Environment variables & global config
    deps.py                 # DuckDB/Redis connection factory (context managers)
    metrics.py              # Prometheus metric definitions
    ratelimit.lua           # Redis rate limiter (token bucket)
  workers/                  # Background tasks & pipeline
    ingest_futu.py          # Incremental Futu ingest (DuckDB raw ticks)
    strategy_runner.py      # Redis Streams VWAP break strategy
    backtest.py             # DuckDB-driven VWAP backtester
    validate.py             # A-group validation (coverage, monotonic timestamps)
    commit.py               # Move to trusted + 1m aggregation
    scheduler.py            # APS scheduler skeleton (for subscription rotation)
  scripts/                  # Utility scripts
    seed_demo.py            # Generate 2-day tick mock data (00700.HK, AAPL)
    init_duckdb.py          # Initialize DuckDB (executes sql/schema.sql)
  sql/
    schema.sql              # Complete DDL (dimensions, staging/trusted, audit)
  grafana/
    dashboard.json          # Example dashboard (Tick Rate/Latency/Validation)
  raw_demo/                 # (Generated) 2-day mock CSV files
  data/                     # (Generated) DuckDB & data lake directory
  .env.sample               # Sample environment variables
  requirements.txt
  README.md
  docs/
    RUNBOOK.md             # Operations manual (common commands)
    PIPELINE.md            # Data flow & state machine
    API.md                 # API reference (MVP)
```

## Module Responsibilities

- **app/**: External service layer (REST/WS + Prometheus); dependency injection (DuckDB/Redis); global config
- **workers/**: Data pipeline (ingest→validate→commit) plus live strategy + backtesting utilities
- **sql/**: Data model & initialization
- **scripts/**: Environment setup & demo data generation
- **grafana/**: Operations visualization templates
- **docs/**: Documentation & operations manual

## Data Flow (2-day demo)

```
Futu OpenD → workers/ingest_futu.py → fact_ticks_raw
                                  ↓
                         workers/validate.py (A-group)
                                  ↓
                         workers/commit.py
                                  ↓
fact_ticks_trusted → fact_bars_1m_trusted → app API (/api/trusted/bars)
                                         ↘ Prometheus /metrics
                                         ↘ Redis Streams → workers/strategy_runner.py → bus:signals:*
DuckDB historical ticks → workers/backtest.py (VWAP replay)
```

## Key Design Decisions

### Database Connection Management

- **API Server**: Uses context managers to open/close read-only connections per request
- **Workers**: Create independent write connections when executing tasks
- **Why**: DuckDB doesn't support concurrent write connections. This design allows API server to run concurrently with workers without lock conflicts

### DuckDB

- Column-oriented storage + local file database
- `schema.sql` includes indexes and views
- Aggregation uses `dt.floor('min')` (pandas) or `DATE_TRUNC('minute', ts_exchange)`
- Schema updated to remove invalid compression settings for compatibility

### Validation

- **A-group rules**:
  - `A_coverage_rows`: Ensures data exists for the date (count > 0)
  - `A_monotonic_ts`: Validates timestamps are monotonically increasing
- Results written to `validation_results` table
- Uses Python `uuid` module for UUID generation (not `duckdb.uuid()`)

### Trusted Data

- `INSERT OR IGNORE` maintains idempotency
- 1-minute aggregation generates `fact_bars_1m_trusted`
- All timestamps properly cast in API queries

### Prometheus

- `/metrics` endpoint automatically exposed
- Future: add rate/latency metrics in workers

### Redis

- Streams now power the demo strategy runner (`workers/strategy_runner.py`) consuming `bus:ticks:<symbol>` and producing `bus:signals:<strategy>:<symbol>`
- `/api/status` and `/dashboard` surface Redis-backed operational state (pinned/focus queues)
- Rate limiter + subscription rotation scaffolding remain for future expansion

### Strategy & Backtest Utilities

- `workers/strategy_runner.py`: Minimal VWAP break strategy with Prometheus metrics and Redis Streams consumer group semantics
- `workers/backtest.py`: Deterministic replay over DuckDB ticks to benchmark the same VWAP logic offline

## Implementation Roadmap

- **P0**: ✅ Complete 2-day loop with CSV mock and API integration
- **P1**: Replace with real Futu API data; add B-group cross-validation
- **P2**: Subscription rotation + Redis Streams real-time
- **P3**: Strategy Runner & Risk, WebUI v0.5, Alerts + Audit

## Recent Changes

- Replaced ingest demo with real Futu incremental ingestion directly into DuckDB raw ticks
- Added Redis Streams strategy runner + Prometheus metrics plus DuckDB backtester
- Introduced `/api/status` + `/dashboard` FastAPI routes backed by Redis state
- Added Longbridge adapter stub to define cross-validation interfaces
- Cleaned requirements + Dockerfile for reproducible environments
