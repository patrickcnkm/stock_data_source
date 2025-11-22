PRAGMA threads=8;
PRAGMA enable_object_cache=true;

CREATE TABLE IF NOT EXISTS dim_symbol_map(
  std_symbol  VARCHAR PRIMARY KEY,
  futu_symbol VARCHAR,
  lb_symbol   VARCHAR,
  market      VARCHAR,
  currency    VARCHAR,
  lot_size    INTEGER,
  tick_size   DOUBLE
);

CREATE TABLE IF NOT EXISTS dim_calendar(
  market      VARCHAR,
  trade_date  DATE,
  session     VARCHAR,
  is_half_day BOOLEAN,
  open_ts     TIMESTAMP,
  close_ts    TIMESTAMP,
  notes       VARCHAR,
  PRIMARY KEY (market, trade_date, session)
);

CREATE TABLE IF NOT EXISTS dim_corporate_actions(
  std_symbol VARCHAR,
  ex_date    DATE,
  type       VARCHAR,
  factor     DOUBLE,
  meta       JSON,
  PRIMARY KEY (std_symbol, ex_date, type)
);

CREATE TABLE IF NOT EXISTS fact_ticks_staging(
  src         VARCHAR,
  std_symbol  VARCHAR,
  ts_exchange TIMESTAMP,
  ts_local    TIMESTAMP,
  price       DOUBLE,
  size        BIGINT,
  trade_id    VARCHAR,
  cond        VARCHAR,
  file_date   DATE,
  PRIMARY KEY (src, std_symbol, ts_exchange, trade_id)
);

CREATE TABLE IF NOT EXISTS fact_ticks_trusted(
  std_symbol  VARCHAR,
  ts_exchange TIMESTAMP,
  price       DOUBLE,
  size        BIGINT,
  trade_id    VARCHAR,
  cond        VARCHAR,
  file_date   DATE,
  PRIMARY KEY (std_symbol, ts_exchange, trade_id)
);

CREATE INDEX IF NOT EXISTS idx_ticks_trusted_symbol_ts
ON fact_ticks_trusted(std_symbol, ts_exchange);

CREATE TABLE IF NOT EXISTS fact_bars_1m_trusted(
  std_symbol  VARCHAR,
  bar_time    TIMESTAMP,
  open        DOUBLE,
  high        DOUBLE,
  low         DOUBLE,
  close       DOUBLE,
  volume      BIGINT,
  vwap        DOUBLE,
  trades      BIGINT,
  file_date   DATE,
  PRIMARY KEY (std_symbol, bar_time)
);

CREATE TABLE IF NOT EXISTS ingest_runs(
  run_id     UUID,
  src        VARCHAR,
  std_symbol VARCHAR,
  file_date  DATE,
  stage      VARCHAR,
  started_at TIMESTAMP,
  ended_at   TIMESTAMP,
  status     VARCHAR,
  details    JSON
);

CREATE TABLE IF NOT EXISTS validation_results(
  run_id     UUID,
  std_symbol VARCHAR,
  file_date  DATE,
  rule_name  VARCHAR,
  metric     DOUBLE,
  threshold  DOUBLE,
  passed     BOOLEAN,
  details    JSON
);

CREATE TABLE IF NOT EXISTS partitions_manifest(
  table_name   VARCHAR,
  std_symbol   VARCHAR,
  file_date    DATE,
  file_path    VARCHAR,
  committed_at TIMESTAMP,
  PRIMARY KEY (table_name, std_symbol, file_date)
);