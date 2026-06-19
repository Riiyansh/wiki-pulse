"""
Spark Structured Streaming — Wikipedia Edit Processor

Reads from Kafka topic "wiki-edits", processes in micro-batches,
writes raw edits + aggregations to PostgreSQL.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, IntegerType, TimestampType
)

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
POSTGRES_URL     = os.environ.get("POSTGRES_URL", "jdbc:postgresql://localhost:5432/wikipulse")
POSTGRES_USER    = os.environ.get("POSTGRES_USER", "wiki")
POSTGRES_PASSWORD= os.environ.get("POSTGRES_PASSWORD", "wiki123")
TOPIC            = "wiki-edits"

PG_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver":   "org.postgresql.Driver",
}

# ── Schema matching producer output ────────────────────────────────────
EDIT_SCHEMA = StructType([
    StructField("event_time",   StringType(),  True),
    StructField("title",        StringType(),  True),
    StructField("wiki",         StringType(),  True),
    StructField("language",     StringType(),  True),
    StructField("user_name",    StringType(),  True),
    StructField("is_bot",       BooleanType(), True),
    StructField("is_new_page",  BooleanType(), True),
    StructField("delta_bytes",  IntegerType(), True),
    StructField("comment",      StringType(),  True),
    StructField("server_url",   StringType(),  True),
    StructField("namespace",    IntegerType(), True),
])


def write_to_postgres(df, table: str):
    df.write.jdbc(url=POSTGRES_URL, table=table, mode="append", properties=PG_PROPS)


def process_batch(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return

    # Parse JSON from Kafka value
    parsed = (
        batch_df
        .select(F.from_json(F.col("value").cast("string"), EDIT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("title").isNotNull())
    )

    count = parsed.count()
    if count == 0:
        return

    print(f"[spark] Batch {batch_id}: {count} edits")

    # ── 1. Write raw edits ──────────────────────────────────────────
    write_to_postgres(parsed.select(
        "event_time", "title", "wiki", "language", "user_name",
        "is_bot", "is_new_page", "delta_bytes", "comment", "server_url", "namespace"
    ), "edits")

    # ── 2. 1-minute stats aggregation ──────────────────────────────
    stats = parsed.groupBy(
        F.window("event_time", "1 minute").alias("w")
    ).agg(
        F.count("*").alias("total_edits"),
        F.sum(F.when(F.col("is_bot"), 1).otherwise(0)).alias("bot_edits"),
        F.sum(F.when(~F.col("is_bot"), 1).otherwise(0)).alias("human_edits"),
        F.sum(F.when(F.col("is_new_page"), 1).otherwise(0)).alias("new_pages"),
        F.countDistinct("user_name").alias("unique_editors"),
        F.first("language").alias("top_language"),
    ).select(
        F.col("w.start").alias("window_start"),
        "total_edits", "bot_edits", "human_edits", "new_pages",
        "unique_editors", "top_language"
    )
    write_to_postgres(stats, "edit_stats_1min")

    # ── 3. Top articles (5-min window) ─────────────────────────────
    top = parsed.groupBy("title", "wiki").agg(
        F.count("*").alias("edit_count"),
        F.min("event_time").alias("window_start"),
    ).withColumn("window_minutes", F.lit(5))

    # Flag spikes: articles with >10 edits in this batch = potential breaking news
    top = top.withColumn("is_spike", F.col("edit_count") > 10)
    write_to_postgres(top.select(
        "window_start", "window_minutes", "title", "wiki", "edit_count", "is_spike"
    ), "top_articles")

    # ── 4. Spike / breaking news detection ─────────────────────────
    spikes = top.filter(F.col("is_spike")).select(
        F.current_timestamp().alias("detected_at"),
        "title", "wiki", "edit_count",
        F.lit(None).cast("double").alias("baseline_avg"),
        F.lit(None).cast("double").alias("spike_ratio"),
        F.lit(True).alias("is_active"),
    )
    if spikes.count() > 0:
        write_to_postgres(spikes, "spikes")
        for row in spikes.collect():
            print(f"[spark] 🔥 SPIKE detected: {row['title']} ({row['edit_count']} edits)")


def main():
    spark = (
        SparkSession.builder
        .appName("WikiPulse")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.postgresql:postgresql:42.6.0")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/wiki-checkpoint")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    query = (
        stream.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[spark] Streaming started — processing every 10s")
    query.awaitTermination()


if __name__ == "__main__":
    main()
