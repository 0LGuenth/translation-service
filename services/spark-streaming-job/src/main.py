import argparse
import os
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


EVENT_SCHEMA = T.StructType(
    [
        T.StructField("req_id", T.StringType(), True),
        T.StructField("src", T.StringType(), True),
        T.StructField("user_id_hashed", T.StringType(), True),
        T.StructField("src_lang", T.StringType(), True),
        T.StructField("tgt_lang", T.StringType(), True),
        T.StructField("char_count", T.IntegerType(), True),
        T.StructField("model", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("latency_ms_total", T.LongType(), True),
        T.StructField("latency_ms_translate", T.LongType(), True),
        T.StructField("event_ts", T.StringType(), True),
        T.StructField("error_type", T.StringType(), True),
    ]
)


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def path_join(root: str, child: str) -> str:
    return root.rstrip("/") + "/" + child.strip("/")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName(env("SPARK_APP_NAME", "translation-spark-streaming-job"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", env("S3_ENDPOINT", "http://seaweedfs-filer.seaweedfs.svc.cluster.local:8333"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    if access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
    if secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)

    return builder.getOrCreate()


def paths() -> dict[str, str]:
    return {
        "bronze": env("BRONZE_PATH", "s3a://translation-bronze/events"),
        "checkpoint_root": env("CHECKPOINT_ROOT", "s3a://translation-checkpoints/spark-streaming-job"),
    }


def kafka_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", "translation-kafka-kafka-bootstrap:9092"))
        .option("subscribe", env("KAFKA_TOPIC", "translation-events"))
        .option("startingOffsets", env("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", env("KAFKA_FAIL_ON_DATA_LOSS", "false"))
        .load()
    )


def parse_kafka_events(raw: DataFrame) -> DataFrame:
    bronze = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_event"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    )
    parsed = bronze.withColumn("event", F.from_json("raw_event", EVENT_SCHEMA))
    return normalize_event_columns(parsed)


def normalize_event_columns(parsed: DataFrame) -> DataFrame:
    df = parsed.select(
        "kafka_key",
        "raw_event",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_ts",
        "ingest_ts",
        F.col("event.req_id").alias("req_id"),
        F.col("event.src").alias("src"),
        F.col("event.user_id_hashed").alias("user_id_hashed"),
        F.lower(F.trim(F.col("event.src_lang"))).alias("src_lang"),
        F.lower(F.trim(F.col("event.tgt_lang"))).alias("tgt_lang"),
        F.col("event.char_count").alias("char_count"),
        F.col("event.model").alias("model"),
        F.lower(F.trim(F.col("event.status"))).alias("status"),
        F.col("event.latency_ms_total").alias("latency_ms_total"),
        F.col("event.latency_ms_translate").alias("latency_ms_translate"),
        F.col("event.event_ts").alias("event_ts"),
        F.to_timestamp(F.col("event.event_ts")).alias("event_time"),
        F.col("event.error_type").alias("error_type"),
    )
    return (
        df.withColumn("effective_ts", F.coalesce(F.col("event_time"), F.col("ingest_ts")))
        .withColumn("date", F.to_date("effective_ts"))
        .withColumn("hour", F.date_format("effective_ts", "HH"))
    )


def start_delta_stream(
    df: DataFrame,
    path: str,
    checkpoint: str,
    query_name: str,
    partition_by: Iterable[str] = (),
):
    writer = (
        df.writeStream.format("delta")
        .queryName(query_name)
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    return writer.start(path)


def run_stream(spark: SparkSession) -> None:
    p = paths()
    raw = kafka_stream(spark)
    parsed = parse_kafka_events(raw)

    queries = [
        start_delta_stream(parsed, p["bronze"], path_join(p["checkpoint_root"], "bronze"), "translation-bronze", ["date", "hour"]),
    ]

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["stream"], default=env("JOB_MODE", "stream"))
    return parser.parse_args()


def main() -> None:
    parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "INFO"))
    run_stream(spark)


if __name__ == "__main__":
    main()
