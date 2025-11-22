API Documentation
=================

## Health Check

**GET** `/api/health`

Health check endpoint to verify the API server and database connectivity.

**Response:**
```json
{"status": "ok"}
```

## Trusted Bars

**GET** `/api/trusted/bars`

Query 1-minute aggregated bars from the trusted dataset.

**Parameters:**
- `symbol` (required): Stock symbol (e.g., `00700.HK`, `AAPL`)
- `start` (required): Start timestamp in format `YYYY-MM-DD HH:MM:SS` (e.g., `2025-11-19 09:30:00`)
- `end` (required): End timestamp in format `YYYY-MM-DD HH:MM:SS` (e.g., `2025-11-19 16:00:00`)

**Example:**
```
GET /api/trusted/bars?symbol=00700.HK&start=2025-11-19 09:30:00&end=2025-11-19 09:45:00
```

**Response:**
Array of tuples containing:
- `std_symbol`: Standardized symbol
- `bar_time`: Timestamp of the bar
- `open`: Opening price
- `high`: Highest price
- `low`: Lowest price
- `close`: Closing price
- `volume`: Total volume
- `vwap`: Volume-weighted average price
- `trades`: Number of trades
- `file_date`: Date of the file

## Validation Results

**GET** `/api/validation/results`

Query validation results for a specific date.

**Parameters:**
- `date` (required): Date in format `YYYY-MM-DD` (e.g., `2025-11-19`)

**Example:**
```
GET /api/validation/results?date=2025-11-19
```

**Response:**
Array of tuples containing:
- `run_id`: UUID of the validation run
- `std_symbol`: Stock symbol
- `file_date`: Date of the validated file
- `rule_name`: Name of the validation rule (e.g., `A_coverage_rows`, `A_monotonic_ts`)
- `metric`: Calculated metric value
- `threshold`: Threshold value for the rule
- `passed`: Boolean indicating if validation passed
- `details`: Additional details (JSON, may be null)

## Prometheus Metrics

**GET** `/metrics`

Returns Prometheus-formatted metrics for monitoring.

## Status Snapshot

**GET** `/api/status`

Reads Redis state to summarize downstream workers (e.g., subscription statistics).

**Response:**
```json
{
  "sub_stat": {"total": "120", "active": "80"},
  "pinned": ["00700.HK", "AAPL"],
  "focus_queue": ["MSFT", "NVDA", "BABA"],
  "focus_current": ["MSFT"]
}
```

Notes:
- Values originate from Redis hashes/sets; missing keys return empty structures.
- Useful for lightweight health probes or the `/dashboard` view.

## WebSocket Quotes

**WS** `/ws/quotes`

WebSocket endpoint for real-time quotes stream (demo implementation).

**Connection:**
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/quotes');
ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(data);
};
```

## Dashboard

**GET** `/dashboard`

Static HTML page that fetches `/api/status` and renders the JSON payload for quick operator insight. Useful when Grafana is unavailable; no authentication or auto-refresh is baked in yet.

## Notes

- All timestamps in the API use format `YYYY-MM-DD HH:MM:SS` for DATETIME fields
- Dates use format `YYYY-MM-DD` for DATE fields
- The API uses read-only database connections per request to avoid conflicts with worker scripts
- B-group validation results are available through the same `/api/validation/results` endpoint after cross-source validation is performed
- `/api/status` requires `REDIS_URL` to be configured (defaults to `redis://localhost:6379/0`)
