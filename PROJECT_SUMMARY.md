# Stock Data Source Platform - Project Summary

## Project Overview

This is a **Quantitative Trading Data Platform (MVP)** designed to collect, validate, store, and serve real-time and historical stock market data for algorithmic trading strategies. The platform implements a complete data pipeline from ingestion to trusted data storage, with support for real-time streaming, validation, and strategy execution.

## Core Objectives

1. **Data Ingestion**: Collect tick-level stock data from multiple sources (Futu OpenD, Longbridge, CSV files)
2. **Data Validation**: Implement multi-tier validation (A-group: coverage/monotonicity, B-group: cross-source comparison)
3. **Data Storage**: Store validated data in DuckDB with staging → trusted pipeline
4. **Real-time Processing**: Stream live market data via Redis Streams for strategy execution
5. **Subscription Management**: Intelligently rotate stock subscriptions within API quota limits
6. **Strategy Execution**: Run trading strategies that consume real-time data streams
7. **Monitoring & Observability**: Prometheus metrics and Grafana dashboards

## Architecture Components

### 1. Data Layer (`sql/schema.sql`, `app/deps.py`)

**Database Schema:**
- **Dimension Tables**: `dim_symbol_map`, `dim_calendar`, `dim_corporate_actions`
- **Staging Tables**: `fact_ticks_staging` (raw ingested data)
- **Trusted Tables**: `fact_ticks_trusted`, `fact_bars_1m_trusted` (validated data)
- **Audit Tables**: `ingest_runs`, `validation_results`, `partitions_manifest`

**Connection Management:**
- Context manager pattern for DuckDB connections
- Read-only connections for API (per-request)
- Independent write connections for workers
- Prevents database lock conflicts

### 2. Application Layer (`app/`)

**Core Services:**
- `main.py`: FastAPI REST API with endpoints for:
  - Health checks (`/api/health`)
  - Trusted bars query (`/api/trusted/bars`)
  - Validation results (`/api/validation/results`)
  - Prometheus metrics (`/metrics`)
  - WebSocket quotes stream (`/ws/quotes`)

**Configuration:**
- `settings.py`: Centralized configuration via Pydantic Settings
  - Database paths, Redis URLs
  - Futu OpenD connection settings
  - Alerting configuration (Feishu webhook, SMTP)
  - Subscription quotas

**Data Adapters:**
- `clients/futu_adapter.py`: Interface to Futu OpenD API for historical/real-time data
- `clients/longbridge_adapter.py`: Interface to Longbridge API (for B-group validation)

**Infrastructure:**
- `metrics.py`: Prometheus metric definitions (tick rates, validation rates, query latency, slippage)
- `stream_consts.py`: Redis Stream key prefixes and naming conventions
- `ratelimit.lua`: Redis Lua script for token bucket rate limiting

### 3. Worker Pipeline (`workers/`)

**Data Ingestion:**
- `ingest_futu.py`: 
  - Demo mode: Reads CSV files from `raw_demo/`
  - Real mode: Fetches data from Futu OpenD API
  - Writes to `fact_ticks_staging`

**Data Validation:**
- `validate.py`: A-group validation
  - Coverage validation (ensures data exists)
  - Monotonic timestamp validation
  - Writes results to `validation_results`
- `validate_cross.py`: B-group cross-source validation
  - Compares Futu data vs Longbridge data
  - Price deviation, volume deviation, coverage ratio checks

**Data Commitment:**
- `commit.py`: Moves validated data from staging to trusted
  - `fact_ticks_staging` → `fact_ticks_trusted`
  - Aggregates to 1-minute bars → `fact_bars_1m_trusted`

**Real-time Processing:**
- `realtime_gateway.py`: Futu OpenD → Redis Streams bridge
  - Subscribes to ticker/quote/orderbook data
  - Publishes to `bus:ticks:{symbol}` streams
  - Handles callbacks from Futu SDK
- `streams_gateway.py`: Historical data → Redis Streams (for simulation)
  - Reads from `fact_ticks_staging`
  - Publishes to Redis Streams with configurable pacing
  - Simulates real-time data flow

**Strategy Execution:**
- `strategy_runner.py`: Consumes Redis Streams and generates trading signals
  - Implements moving average crossover strategy (5/15 period)
  - Reads from `bus:ticks:{symbol}` streams
  - Publishes signals to `bus:signal` stream
  - Uses Redis Consumer Groups for parallel processing

**Subscription Management:**
- `sub_rotation.py`: Demo subscription rotation
  - Manages pinned symbols (always subscribed)
  - Rotates through candidate symbols in batches
  - Mock implementation for testing
- `sub_rotation_real.py`: Production subscription rotation
  - Manages Futu OpenD subscription quota (300 total)
  - Enforces HK orderbook limit (48 concurrent)
  - Debouncing (≥60s between rotations)
  - Exponential backoff on failures
  - Prometheus metrics for monitoring
  - Alerting integration

**Orchestration:**
- `scheduler.py`: APScheduler-based job scheduler
  - Coordinates multiple workers
  - Can schedule periodic ingest/validate/commit jobs

### 4. Scripts (`scripts/`)

- `init_duckdb.py`: Initializes DuckDB database with schema
- `seed_demo.py`: Generates mock CSV data for testing (2 days, 2 symbols)

### 5. Monitoring & Observability

**Prometheus (`prometheus.yml`):**
- Scrapes FastAPI metrics endpoint
- Configurable scrape intervals

**Grafana (`grafana/dashboard.json`):**
- Pre-configured dashboards for:
  - Tick ingestion rates
  - DuckDB query latency (P95)
  - Validation pass rates
  - Subscription rotation metrics

**Docker Compose (`docker-compose.yml`):**
- Prometheus container (port 9090)
- Grafana container (port 3000)
- Redis (optional, can use local)

## Data Flow

### Batch Pipeline (Historical Data)
```
CSV/Futu API → ingest_futu.py → fact_ticks_staging
                                    ↓
                            validate.py (A-group)
                                    ↓
                            validate_cross.py (B-group, optional)
                                    ↓
                            commit.py → fact_ticks_trusted
                                    ↓
                            fact_bars_1m_trusted (1-minute aggregation)
                                    ↓
                            FastAPI /api/trusted/bars
```

### Real-time Pipeline
```
Futu OpenD → realtime_gateway.py → Redis Streams (bus:ticks:{symbol})
                                            ↓
                                    strategy_runner.py
                                            ↓
                                    Redis Streams (bus:signal)
```

### Subscription Rotation Flow
```
sub_rotation_real.py → Query Futu quota
                    → Calculate available budget
                    → Select next batch from queue
                    → Subscribe to new symbols
                    → Hold for configured duration
                    → Unsubscribe from previous batch
                    → Rotate queue
```

## Key Features

### 1. Multi-Source Data Ingestion
- **Futu OpenD**: Real-time and historical data for Hong Kong and US markets
- **Longbridge**: Alternative data source for cross-validation
- **CSV Files**: Demo/testing mode

### 2. Multi-Tier Validation
- **A-Group (Longitudinal)**: 
  - Coverage validation (data completeness)
  - Monotonic timestamp validation (data integrity)
- **B-Group (Cross-Source)**:
  - Price deviation validation (P95 percentile)
  - Volume deviation validation
  - Coverage ratio validation

### 3. Data Quality Pipeline
- **Staging Layer**: Raw ingested data (can be re-validated)
- **Trusted Layer**: Validated data only
- **Audit Trail**: Complete validation history in `validation_results`

### 4. Real-time Streaming Architecture
- **Redis Streams**: High-throughput message streaming
- **Consumer Groups**: Parallel strategy execution
- **Backpressure Handling**: Configurable stream maxlen

### 5. Intelligent Subscription Management
- **Quota Management**: Respects Futu API limits (300 total subscriptions)
- **HK Orderbook Limit**: Enforces 48 concurrent HK orderbook subscriptions
- **Pinned Symbols**: Always-subscribed symbols (e.g., major indices)
- **Batch Rotation**: Efficiently rotates through candidate symbols
- **Debouncing**: Prevents rapid subscription changes

### 6. Strategy Framework
- **Streaming Consumers**: Read from Redis Streams
- **Signal Generation**: Publish trading signals
- **Extensible**: Easy to add new strategies

### 7. Observability
- **Prometheus Metrics**: 
  - Tick ingestion rates
  - Validation pass rates
  - Query latency
  - Subscription usage
  - API call latency
- **Grafana Dashboards**: Visual monitoring
- **Alerting**: Feishu webhook and SMTP support

## Technology Stack

- **FastAPI**: REST API framework
- **DuckDB**: Columnar analytical database
- **Redis**: Streams and caching
- **Prometheus**: Metrics collection
- **Grafana**: Visualization
- **APScheduler**: Job scheduling
- **Futu API**: Market data provider
- **Pandas**: Data manipulation
- **Pydantic**: Settings management

## Use Cases

1. **Historical Data Analysis**: Query validated 1-minute bars for backtesting
2. **Real-time Strategy Execution**: Run algorithms on live market data
3. **Data Quality Assurance**: Validate data from multiple sources
4. **Market Monitoring**: Track tick rates, validation rates, system health
5. **Subscription Optimization**: Efficiently manage API quota across many symbols

## Project Status

**MVP Status**: ✅ Complete and functional
- All core pipelines implemented
- API endpoints working
- Real-time streaming operational
- Subscription rotation functional
- Monitoring infrastructure ready

**Future Enhancements** (per roadmap):
- P1: Full Longbridge integration for B-group validation
- P2: Enhanced subscription rotation with more sophisticated algorithms
- P3: Strategy risk management, WebUI, enhanced alerting

## Key Design Decisions

1. **DuckDB over traditional databases**: Fast analytical queries, columnar storage, local file-based
2. **Redis Streams over Kafka**: Simpler setup, sufficient throughput for MVP
3. **Context managers for DB connections**: Prevents lock conflicts, enables concurrent API + workers
4. **Staging → Trusted pipeline**: Allows re-validation, maintains data lineage
5. **Multi-tier validation**: Ensures data quality at multiple levels
6. **Subscription rotation**: Maximizes data coverage within API limits

This platform provides a production-ready foundation for quantitative trading data management, with clear extension points for additional data sources, validation rules, and trading strategies.




