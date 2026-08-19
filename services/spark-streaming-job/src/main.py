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

LANGUAGE_SCHEMA = T.StructType(
    [
        T.StructField("code", T.StringType(), False),
        T.StructField("name", T.StringType(), False),
        T.StructField("script", T.StringType(), False),
        T.StructField("family", T.StringType(), False),
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
        "silver": env("SILVER_PATH", "s3a://translation-silver/events"),
        "silver_invalid": env("SILVER_INVALID_PATH", "s3a://translation-silver/invalid-events"),
        "gold_1m": env("GOLD_1M_PATH", "s3a://translation-gold/language-pair-1m"),
        "gold_5m": env("GOLD_5M_PATH", "s3a://translation-gold/language-pair-5m"),
        "gold_user_alerts": env("GOLD_USER_ALERTS_PATH", "s3a://translation-gold/user-alerts"),
        "checkpoint_root": env("CHECKPOINT_ROOT", "s3a://translation-checkpoints/spark-streaming-job"),
        "languages": env("LANGUAGES_PATH", "/opt/translation/spark-streaming-job/src/languages.json"),
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


def parse_bronze_events(bronze: DataFrame) -> DataFrame:
    if "raw_event" not in bronze.columns:
        raise ValueError("Bronze table must contain raw_event")
    parsed = bronze.withColumn("event", F.from_json("raw_event", EVENT_SCHEMA))
    if "ingest_ts" not in parsed.columns:
        parsed = parsed.withColumn("ingest_ts", F.current_timestamp())
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


def with_validation(df: DataFrame) -> DataFrame:
    error_text = F.concat_ws(
        ",",
        F.when(F.col("req_id").isNull() | (F.length(F.col("req_id")) == 0), "missing_req_id"),
        F.when(F.col("event_time").isNull(), "invalid_event_ts"),
        F.when(F.col("user_id_hashed").isNull() | (F.length(F.col("user_id_hashed")) == 0), "missing_user_id_hashed"),
        F.when(~F.col("src_lang").rlike("^[a-z]{2,5}$"), "invalid_src_lang"),
        F.when(~F.col("tgt_lang").rlike("^[a-z]{2,5}$"), "invalid_tgt_lang"),
        F.when(~F.col("status").isin("success", "error"), "invalid_status"),
        F.when(F.col("char_count").isNull() | (F.col("char_count") < 0), "invalid_char_count"),
    )
    return df.withColumn("validation_errors", error_text)


def load_languages(spark: SparkSession, languages_path: str) -> DataFrame:
    return (
        spark.read.schema(LANGUAGE_SCHEMA)
        .json(languages_path)
        .withColumn("code", F.lower(F.trim("code")))
    )


def silver_events(parsed: DataFrame, languages: DataFrame) -> tuple[DataFrame, DataFrame]:
    checked = with_validation(parsed)
    invalid = checked.filter(F.col("validation_errors") != "")

    valid = (
        checked.filter(F.col("validation_errors") == "")
        .withColumn("language_pair", F.concat_ws("-", "src_lang", "tgt_lang"))
        .drop("validation_errors")
    )

    src_langs = languages.select(
        F.col("code").alias("src_lang"),
        F.col("name").alias("src_lang_name"),
        F.col("script").alias("src_script"),
        F.col("family").alias("src_family"),
    )
    tgt_langs = languages.select(
        F.col("code").alias("tgt_lang"),
        F.col("name").alias("tgt_lang_name"),
        F.col("script").alias("tgt_script"),
        F.col("family").alias("tgt_family"),
    )

    enriched = valid.join(src_langs, on="src_lang", how="left").join(tgt_langs, on="tgt_lang", how="left")
    return enriched, invalid


def language_pair_agg(df: DataFrame, window_duration: str, slide_duration: str | None = None) -> DataFrame:
    window_col = F.window("event_time", window_duration, slide_duration) if slide_duration else F.window("event_time", window_duration)
    source = df.withWatermark("event_time", env("WATERMARK_DELAY", "30 seconds")) if df.isStreaming else df
    grouped = (
        source
        .groupBy(window_col.alias("window"), "language_pair")
        .agg(
            F.count("*").alias("request_count"),
            F.avg("latency_ms_total").alias("avg_latency_ms_total"),
            F.avg("latency_ms_translate").alias("avg_latency_ms_translate"),
            F.sum(F.when(F.col("status") != "success", 1).otherwise(0)).alias("error_count"),
            F.expr("percentile_approx(latency_ms_total, 0.95)").alias("p95_latency_ms_total"),
        )
    )
    return (
        grouped.withColumn("error_rate", F.col("error_count") / F.col("request_count"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("date", F.to_date("window_start"))
        .drop("window")
    )


def user_alerts(df: DataFrame) -> DataFrame:
    threshold = int(env("USER_ALERT_THRESHOLD", "20"))
    source = df.withWatermark("event_time", env("WATERMARK_DELAY", "30 seconds")) if df.isStreaming else df
    grouped = (
        source
        .groupBy(F.window("event_time", "5 minutes").alias("window"), "user_id_hashed")
        .agg(F.count("*").alias("request_count"), F.approx_count_distinct("language_pair").alias("language_pair_count"))
        .filter(F.col("request_count") >= threshold)
    )
    return (
        grouped.withColumn("alert_type", F.lit("many_requests_per_user_5m"))
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("date", F.to_date("window_start"))
        .drop("window")
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


def start_gold_stream(df: DataFrame, path: str, checkpoint: str, query_name: str):
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        output = batch_df.withColumn("batch_id", F.lit(batch_id))
        output.write.format("delta").mode("append").partitionBy("date").save(path)

    return (
        df.writeStream.queryName(query_name)
        .outputMode("update")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(write_batch)
        .start()
    )


def run_stream(spark: SparkSession) -> None:
    p = paths()
    raw = kafka_stream(spark)
    parsed = parse_kafka_events(raw)
    languages = load_languages(spark, p["languages"])
    silver, invalid = silver_events(parsed, languages)

    queries = [
        start_delta_stream(parsed, p["bronze"], path_join(p["checkpoint_root"], "bronze"), "translation-bronze", ["date", "hour"]),
        start_delta_stream(silver, p["silver"], path_join(p["checkpoint_root"], "silver"), "translation-silver", ["date", "language_pair"]),
        start_delta_stream(invalid, p["silver_invalid"], path_join(p["checkpoint_root"], "silver-invalid"), "translation-silver-invalid", ["date"]),
        start_gold_stream(language_pair_agg(silver, "1 minute"), p["gold_1m"], path_join(p["checkpoint_root"], "gold-1m"), "translation-gold-1m"),
        start_gold_stream(language_pair_agg(silver, "5 minutes", "1 minute"), p["gold_5m"], path_join(p["checkpoint_root"], "gold-5m"), "translation-gold-5m"),
        start_gold_stream(user_alerts(silver), p["gold_user_alerts"], path_join(p["checkpoint_root"], "gold-user-alerts"), "translation-gold-user-alerts"),
    ]

    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()


def write_delta(df: DataFrame, path: str, partition_by: Iterable[str] = ()) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def run_reprocess(spark: SparkSession) -> None:
    p = paths()
    bronze = spark.read.format("delta").load(p["bronze"])
    parsed = parse_bronze_events(bronze)
    languages = load_languages(spark, p["languages"])
    silver, invalid = silver_events(parsed, languages)

    write_delta(silver, p["silver"], ["date", "language_pair"])
    write_delta(invalid, p["silver_invalid"], ["date"])
    gold_1m = language_pair_agg(silver, "1 minute")
    gold_5m = language_pair_agg(silver, "5 minutes", "1 minute")
    alerts = user_alerts(silver)
    write_delta(gold_1m, p["gold_1m"], ["date"])
    write_delta(gold_5m, p["gold_5m"], ["date"])
    write_delta(alerts, p["gold_user_alerts"], ["date"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["stream", "reprocess"], default=env("JOB_MODE", "stream"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel(env("SPARK_LOG_LEVEL", "INFO"))
    if args.mode == "reprocess":
        run_reprocess(spark)
    else:
        run_stream(spark)


if __name__ == "__main__":
    main()
