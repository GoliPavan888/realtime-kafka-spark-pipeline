FROM apache/spark:3.5.1

USER root

# Install Python + dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install pyspark psycopg2-binary kafka-python

# Add PostgreSQL JDBC driver explicitly (NO Ivy issues)
RUN curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /app

CMD spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  stream_app.py
