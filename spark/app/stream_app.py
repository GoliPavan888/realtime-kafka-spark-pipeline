from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, approx_count_distinct,
    to_date, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
import psycopg2

# ==================================================
# Spark Session
# ==================================================
spark = (
    SparkSession.builder
    .appName("KafkaSparkPipeline")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "4")

# ==================================================
# Schema
# ==================================================
schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True)
])

# ==================================================
# Kafka Source
# ==================================================
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "user_activity")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")   # ⭐ IMPORTANT
    .load()
)


parsed = (
    raw_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withWatermark("event_time", "2 minutes")
)

# ==================================================
# PostgreSQL UPSERT HELPERS
# ==================================================
def get_pg_conn():
    return psycopg2.connect(
        host="db",
        dbname="stream_data",
        user="postgres",
        password="postgres"
    )

def upsert_page_views(df, batch_id):
    if df.rdd.isEmpty():
        return

    conn = get_pg_conn()
    cur = conn.cursor()

    for row in df.collect():
        cur.execute(
            """
            INSERT INTO page_view_counts
            (window_start, window_end, page_url, view_count)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (window_start, page_url)
            DO UPDATE SET
                view_count = EXCLUDED.view_count,
                window_end = EXCLUDED.window_end;
            """,
            (row.window_start, row.window_end, row.page_url, row.view_count)
        )

    conn.commit()
    cur.close()
    conn.close()

def upsert_active_users(df, batch_id):
    if df.rdd.isEmpty():
        return

    conn = get_pg_conn()
    cur = conn.cursor()

    for row in df.collect():
        cur.execute(
            """
            INSERT INTO active_users
            (window_start, window_end, active_user_count)
            VALUES (%s, %s, %s)
            ON CONFLICT (window_start)
            DO UPDATE SET
                active_user_count = EXCLUDED.active_user_count,
                window_end = EXCLUDED.window_end;
            """,
            (row.window_start, row.window_end, row.active_user_count)
        )

    conn.commit()
    cur.close()
    conn.close()

# ==================================================
# 1️⃣ PAGE VIEW COUNTS (1-MIN TUMBLING WINDOW)
# ==================================================
page_views_agg = (
    parsed
    .filter(col("event_type") == "page_view")
    .groupBy(
        window(col("event_time"), "1 minute").alias("w"),
        col("page_url")
    )
    .count()
    .select(
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        col("page_url"),
        col("count").alias("view_count")
    )
)

page_view_query = (
    page_views_agg
    .writeStream
    .outputMode("append")
    .foreachBatch(upsert_page_views)
    .option("checkpointLocation", "/tmp/page_view_checkpoint")
    .start()
)

# ==================================================
# 2️⃣ ACTIVE USERS (5-MIN SLIDING, 1-MIN SLIDE)
# ==================================================
active_users_agg = (
    parsed
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute").alias("w")
    )
    .agg(
        approx_count_distinct("user_id").alias("active_user_count")
    )
    .select(
        col("w.start").alias("window_start"),
        col("w.end").alias("window_end"),
        col("active_user_count")
    )
)

active_users_query = (
    active_users_agg
    .writeStream
    .outputMode("append")
    .foreachBatch(upsert_active_users)
    .option("checkpointLocation", "/tmp/active_users_checkpoint")
    .start()
)

# ==================================================
# 3️⃣ DATA LAKE (PARQUET, PARTITIONED)
# ==================================================
lake_query = (
    parsed
    .withColumn("event_date", to_date(col("event_time")))
    .writeStream
    .format("parquet")
    .option("path", "/opt/spark/data/lake")
    .option("checkpointLocation", "/opt/spark/data/lake/_checkpoint")
    .partitionBy("event_date")
    .start()
)

# ==================================================
# 4️⃣ ENRICHED EVENTS → KAFKA
# ==================================================
enriched_query = (
    parsed
    .withColumn("processing_time", current_timestamp())
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "enriched_activity")
    .option("checkpointLocation", "/tmp/enriched_checkpoint")
    .start()
)

# ==================================================
# KEEP STREAMING ALIVE
# ==================================================
spark.streams.awaitAnyTermination()
