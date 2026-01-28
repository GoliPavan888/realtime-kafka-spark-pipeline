from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    approx_count_distinct, to_date,
    current_timestamp, to_json, struct
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, LongType
)
from pyspark.sql.streaming import GroupState
import os
from datetime import datetime

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
# RAW EVENTS ? DATA LAKE
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
# ENRICH EVENTS ? KAFKA
# --------------------------------------------------
enriched_df = events_df.withColumn(
    'processing_time', current_timestamp()
)

enriched_kafka_df = enriched_df.select(
    to_json(struct(
        col('event_time').cast('string'),
        col('user_id'),
        col('page_url'),
        col('event_type'),
        col('processing_time').cast('string')
    )).alias('value')
)

enriched_kafka_df.writeStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'kafka:9092') \
    .option('topic', 'enriched_activity') \
    .option('checkpointLocation', '/opt/spark/data/lake/_checkpoints/enriched') \
    .outputMode('append') \
    .start()

spark.streams.awaitAnyTermination()
