from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window,
    approx_count_distinct, to_date
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, LongType
)
from pyspark.sql.streaming import GroupState, GroupStateTimeout
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
# SESSION STATE FUNCTION
# --------------------------------------------------
session_schema = StructType([
    StructField('user_id', StringType()),
    StructField('session_start_time', TimestampType()),
    StructField('session_end_time', TimestampType()),
    StructField('session_duration_seconds', LongType())
])

def session_state_func(user_id, rows, state: GroupState):
    output = []

    if state.hasTimedOut:
        start_time = state.get()
        end_time = datetime.utcnow()
        duration = int((end_time - start_time).total_seconds())

        output.append((user_id, start_time, end_time, duration))
        state.remove()
        return output

    for row in rows:
        if row.event_type == 'session_start':
            state.update(row.event_time)
            state.setTimeoutDuration(15 * 60 * 1000)

        elif row.event_type == 'session_end' and state.exists:
            start_time = state.get()
            end_time = row.event_time
            duration = int((end_time - start_time).total_seconds())

            output.append((user_id, start_time, end_time, duration))
            state.remove()

    return output

# --------------------------------------------------
# APPLY STATEFUL SESSION LOGIC
# --------------------------------------------------
sessions_df = events_df \
    .filter(col('event_type').isin('session_start', 'session_end')) \
    .groupByKey(lambda r: r.user_id) \
    .flatMapGroupsWithState(
        outputMode='append',
        updateFunction=session_state_func,
        stateTimeoutDuration='15 minutes'
    )

# --------------------------------------------------
# WRITE SESSIONS TO POSTGRES
# --------------------------------------------------
def write_sessions(batch_df, batch_id):
    batch_df.write \
        .format('jdbc') \
        .option('url', DB_URL) \
        .option('dbtable', 'user_sessions') \
        .option('user', DB_USER) \
        .option('password', DB_PASSWORD) \
        .option('driver', 'org.postgresql.Driver') \
        .mode('append') \
        .save()

sessions_df.writeStream \
    .foreachBatch(write_sessions) \
    .option('checkpointLocation', '/opt/spark/data/lake/_checkpoints/sessions') \
    .start()

spark.streams.awaitAnyTermination()
