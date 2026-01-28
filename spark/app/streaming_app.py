from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    approx_count_distinct, to_date
)
from pyspark.sql.types import StructType, StructField, StringType
import os

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName('RealTimeKafkaSparkPipeline') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# --------------------------------------------------
# Environment Variables
# --------------------------------------------------
DB_URL = os.getenv('DB_URL')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# --------------------------------------------------
# Kafka Source
# --------------------------------------------------
kafka_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:9092') \
    .option('subscribe', 'user_activity') \
    .option('startingOffsets', 'latest') \
    .load()

# --------------------------------------------------
# Schema
# --------------------------------------------------
event_schema = StructType([
    StructField('event_time', StringType(), True),
    StructField('user_id', StringType(), True),
    StructField('page_url', StringType(), True),
    StructField('event_type', StringType(), True)
])

# --------------------------------------------------
# Parse + Watermark
# --------------------------------------------------
events_df = kafka_df.select(
    from_json(col('value').cast('string'), event_schema).alias('data')
).select(
    to_timestamp(col('data.event_time')).alias('event_time'),
    col('data.user_id'),
    col('data.page_url'),
    col('data.event_type')
).withWatermark('event_time', '2 minutes')

# --------------------------------------------------
# RAW  DATA LAKE
# --------------------------------------------------
events_df \
    .withColumn('event_date', to_date(col('event_time'))) \
    .writeStream \
    .format('parquet') \
    .option('path', '/opt/spark/data/lake') \
    .option('checkpointLocation', '/opt/spark/data/lake/_checkpoints/raw') \
    .partitionBy('event_date') \
    .outputMode('append') \
    .start()

# --------------------------------------------------
# PAGE VIEW COUNTS
# --------------------------------------------------
page_views = events_df \
    .filter(col('event_type') == 'page_view') \
    .groupBy(
        window(col('event_time'), '1 minute'),
        col('page_url')
    ) \
    .count()

def write_page_views(batch_df, batch_id):
    batch_df.select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('page_url'),
        col('count').alias('view_count')
    ).write \
        .format('jdbc') \
        .option('url', DB_URL) \
        .option('dbtable', 'page_view_counts') \
        .option('user', DB_USER) \
        .option('password', DB_PASSWORD) \
        .option('driver', 'org.postgresql.Driver') \
        .mode('append') \
        .save()

page_views.writeStream \
    .outputMode('update') \
    .foreachBatch(write_page_views) \
    .option('checkpointLocation', '/opt/spark/data/lake/_checkpoints/page_views') \
    .start()

# --------------------------------------------------
# ACTIVE USERS (5-MIN SLIDING WINDOW)
# --------------------------------------------------
active_users = events_df.groupBy(
    window(col('event_time'), '5 minutes', '1 minute')
).agg(
    approx_count_distinct('user_id').alias('active_user_count')
)

def write_active_users(batch_df, batch_id):
    batch_df.select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('active_user_count')
    ).write \
        .format('jdbc') \
        .option('url', DB_URL) \
        .option('dbtable', 'active_users') \
        .option('user', DB_USER) \
        .option('password', DB_PASSWORD) \
        .option('driver', 'org.postgresql.Driver') \
        .mode('append') \
        .save()

active_users.writeStream \
    .outputMode('update') \
    .foreachBatch(write_active_users) \
    .option('checkpointLocation', '/opt/spark/data/lake/_checkpoints/active_users') \
    .start()

spark.streams.awaitAnyTermination()
