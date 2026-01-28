FROM bitnami/spark:3.5.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir pyspark kafka-python psycopg2-binary

# Set working directory
WORKDIR /app

USER 1001
