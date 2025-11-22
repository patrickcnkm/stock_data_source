from prometheus_client import Counter, Histogram, Gauge

tick_ingestion_rate = Gauge('tick_ingestion_rate', 'Ticks per second', ['symbol'])
validation_pass_rate = Gauge('validation_pass_rate', 'Validation pass ratio', ['stage'])
cross_price_dev_p95 = Gauge('cross_price_dev_p95', 'Cross price deviation P95 bps', ['symbol'])

duckdb_query_latency = Histogram('duckdb_query_latency_ms', 'DuckDB query latency (ms)')

order_fill_rate = Gauge('order_fill_rate', 'Order fill rate', ['strategy'])
slippage_bps = Histogram('slippage_bps', 'Slippage bps', ['strategy'])