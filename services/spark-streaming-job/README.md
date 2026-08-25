# Spark Streaming Job

Liest `translation-events` und `translation-errors` aus Kafka und schreibt ein kleines Lakehouse als Delta Lake auf SeaweedFS:

- Bronze: rohe Kafka-Events plus technische Kafka-Metadaten
- Silver: validierte, normalisierte und mit Sprachmetadaten angereicherte Events
- Gold: 1-Minuten- und 5-Minuten-Aggregate pro Sprachpaar sowie User-Burst-Alerts

Gold wird zusaetzlich nach PostgreSQL gespiegelt, damit eine `analytics-api` schnelle Dashboard-Abfragen machen kann.

## Modi

Streaming-Modus:

```bash
JOB_MODE=stream
```

Reprocessing-Modus:

```bash
JOB_MODE=reprocess
```

Der Reprocessing-Modus liest Bronze erneut und berechnet Silver und Gold neu.

## Wichtige Umgebungsvariablen

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC
KAFKA_STARTING_OFFSETS
S3_ENDPOINT
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
BRONZE_PATH
SILVER_PATH
SILVER_INVALID_PATH
GOLD_1M_PATH
GOLD_5M_PATH
GOLD_USER_ALERTS_PATH
CHECKPOINT_ROOT
WATERMARK_DELAY
USER_ALERT_THRESHOLD
POSTGRES_SINK_ENABLED
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

Das Kubernetes-Deployment wird vom Helm-Subchart gerendert:

```text
charts/translate-platform/charts/spark-streaming-job/
```
