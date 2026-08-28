# Analytics API

HTTP API fuer Dashboard-Kennzahlen aus PostgreSQL.

## Endpunkte

```text
GET /health
GET /ready
GET /metrics/summary
GET /metrics/language-pairs
GET /metrics/latency
GET /metrics/errors
GET /metrics/timeseries
```

Die API liest aus den Tabellen, die der Spark-Job befuellt:

```text
language_pair_windows
global_live_metrics
user_alerts
```

## Konfiguration

Entweder `DATABASE_URL` setzen oder einzelne Variablen:

```text
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

## Lokal testen

```bash
GOTOOLCHAIN=local go test ./...
```
