from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp,
    window, count, approx_count_distinct, to_date
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# --------------------------------------------------
# Spark Session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName('RealTimeKafkaSparkPipeline') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

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
# Schema Definition
# --------------------------------------------------
event_schema = StructType([
    StructField('event_time', StringType(), True),
    StructField('user_id', StringType(), True),
    StructField('page_url', StringType(), True),
    StructField('event_type', StringType(), True)
])

# --------------------------------------------------
# Parse JSON and Apply Schema
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
# Write Raw Events to Data Lake (Parquet)
# --------------------------------------------------
raw_query = events_df \
    .withColumn('event_date', to_date(col('event_time'))) \
    .writeStream \
    .format('parquet') \
    .option('path', '/opt/spark/data/lake') \
    .option('checkpointLocation', '/opt/spark/data/lake/_checkpoints/raw') \
    .partitionBy('event_date') \
    .outputMode('append') \
    .start()

raw_query.awaitTermination()
