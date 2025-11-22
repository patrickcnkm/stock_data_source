Operations Runbook
==================

## Initial Setup

```bash
# 1. Create virtual environment and install dependencies
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Create required directories
mkdir -p data/lake raw_demo

# 3. Initialize database schema
python scripts/init_duckdb.py
```

Note: The schema has been updated to remove invalid DuckDB compression settings for compatibility.

## Generate Demo Data (2-day)

```bash
python scripts/seed_demo.py
```

This creates mock CSV files for 00700.HK and AAPL under `raw_demo/` for the past 2 days.

## Execute Complete Pipeline

**Important**: Set `PYTHONPATH` to project root for worker scripts to find the `app` module.

```bash
# Set PYTHONPATH (required for worker scripts)
export PYTHONPATH="."

# 1. Ingest real ticks from Futu OpenD (incremental by date)
python workers/ingest_futu.py --symbols 00700.HK AAPL --days 2

# 2. Validate A-group rules
python workers/validate.py --run-a --symbols 00700.HK AAPL --days 2

# 3. Commit to trusted tables + 1m bars
python workers/commit.py --symbols 00700.HK AAPL --days 2
```

**Demo fallback**: add `--demo` to `ingest_futu.py` if OpenD is unavailable to load CSVs from `raw_demo/`.

## Start API Server

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Endpoints:**
- Swagger/OpenAPI docs: http://localhost:8000/docs
- Health check: http://localhost:8000/api/health
- Status JSON: http://localhost:8000/api/status
- Dashboard: http://localhost:8000/dashboard
- Metrics: http://localhost:8000/metrics
- Trusted bars: http://localhost:8000/api/trusted/bars?symbol=00700.HK&start=2025-11-19 09:30:00&end=2025-11-19 16:00:00
- Validation results: http://localhost:8000/api/validation/results?date=2025-11-19

**Note**: The API server uses read-only connections per request, so it can run concurrently with worker scripts without conflicts.

## Running Workers While API is Running

The connection management has been updated to allow concurrent execution:
- **API Server**: Uses read-only connections per request (context managers)
- **Workers**: Create independent write connections when executing

You can safely run worker scripts while the API server is running.

## Common Issues

### ModuleNotFoundError: No module named 'app'

**Solution**: Set `PYTHONPATH` to project root:
```bash
export PYTHONPATH="."
# or
PYTHONPATH="." python workers/ingest_futu.py --demo ...
```

### DuckDB lock conflicts

**Solution**: The connection management has been fixed. Ensure you're using the updated code where:
- API server uses context managers for read-only connections
- Workers create their own write connections independently

### Invalid DuckDB compression setting

**Solution**: The schema has been updated. Re-initialize the database:
```bash
rm data/quant.duckdb  # Optional: remove old database
python scripts/init_duckdb.py
```

### Timestamp format errors in API

**Solution**: Use format `YYYY-MM-DD HH:MM:SS` for timestamps in API calls:
```
/api/trusted/bars?symbol=00700.HK&start=2025-11-19 09:30:00&end=2025-11-19 16:00:00
```

## Database File Location

- Default: `data/quant.duckdb`
- Can be changed via `.env` file setting `DUCKDB_PATH`


真实采集（Futu）与横向校验（B组）
--------------------------------
# 1) 真实采集（需要安装 futu-api 并启动 OpenD，配置 .env）
python workers/ingest_futu.py --symbols 00700.HK AAPL --days 2

# 2) B 组校验（LongbridgeAdapter 仍为占位，需要真实实现后移除 --demo）
python workers/validate_cross.py --symbols 00700.HK AAPL --days 2 --demo


打通 Redis Streams 与策略 Runner（演示）
--------------------------------------
# 0) 先完成 ingest/validate/commit（或 demo ingest）

# 1) 发布两天的 staging ticks 到 Streams（模拟实时）
python workers/streams_gateway.py --symbols 00700.HK AAPL --days 2 --sleep-ms 2

# 2) 启动策略 Runner（消费 bus:ticks:* 并写入 bus:signals）
python workers/strategy_runner.py --symbol 00700.HK --group strat_vwap_break --consumer worker-1

# 3) 启动 Prometheus 端口监控（默认 9110，flag --metrics-port 可调）
python workers/strategy_runner.py --symbol AAPL --consumer worker-2 --metrics-port 9111

# 3) 订阅轮转（演示模式）
python workers/sub_rotation.py --demo --pinned 00700.HK AAPL --batch-size 6 --hold-sec 75

# 4) 一键通过 APS 调度器启动上述三个组件
python workers/scheduler.py

回测 VWAP 策略（DuckDB）
-----------------------
# 使用 trusted ticks 复盘
python workers/backtest.py --symbol 00700.HK --start 2025-11-19 --end 2025-11-21 --window 20 --eps 0.001

输出示例：
```
[backtest] 00700.HK 2025-11-19 ~ 2025-11-21
{'trades': 14, 'roundtrips': 7, 'pnl': 1.42, 'win_rate': 0.57}
```

## 真实订阅轮转（Futu OpenD）
前置：`pip install futu-api redis prometheus-client python-dotenv` 且 OpenD 已运行；`.env` 需要：
- FUTU_OPEND_IP=127.0.0.1
- FUTU_OPEND_QUOTE_PORT=11111
- FUTU_SUB_TOTAL=300
- REDIS_URL=redis://localhost:6379/0

运行：
python workers/sub_rotation_real.py --pinned 00700.HK AAPL \
  --candidates MSFT GOOG NVDA META BABA 3690.HK 0700.HK TSLA AMD 9988.HK \
  --batch-size 40 --hold-sec 75 --metrics-port 9109

Prometheus 指标（默认 http://localhost:9109/metrics）：
- futu_sub_used / futu_sub_remain
- futu_sub_orderbook_hk
- futu_sub_rotate_success_total / futu_sub_rotate_fail_total{phase}
- futu_sub_last_switch_timestamp / futu_sub_hold_seconds
- futu_api_latency_ms_bucket

### 实时网关（Futu 回调 → Redis Streams）
```bash
python workers/realtime_gateway.py --symbols 00700.HK AAPL --with-orderbook
```
