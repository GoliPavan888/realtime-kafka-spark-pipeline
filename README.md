# Real-Time Kafka → Spark → PostgreSQL Pipeline

[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-000?style=for-the-badge&logo=apachekafka)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

This project demonstrates a real-time data pipeline using Apache Kafka, Apache Spark Structured Streaming, and PostgreSQL, fully containerized with Docker Compose.

The pipeline ingests user activity events, performs streaming analytics, stores aggregations in PostgreSQL, writes raw data to a data lake, and publishes enriched events back to Kafka.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Environment Variables](#environment-variables)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [Verify Outputs](#verify-outputs)
- [Streaming Logic](#streaming-logic)
- [Data Lake](#data-lake)
- [Automatic Setup](#automatic-setup)
- [Troubleshooting](#troubleshooting)
- [Final Notes](#final-notes)
- [Author](#author)
- [License](#license)

## Architecture Overview

```
Producer (Python)
      |
      v
Kafka (user_activity topic)
      |
      v
Spark Structured Streaming
      ├── Page View Aggregation → PostgreSQL
      ├── Active Users Aggregation → PostgreSQL
      ├── Raw Events → Parquet Data Lake
      └── Enriched Events → Kafka (enriched_activity topic)
```

## Tech Stack

- **Apache Kafka** – Event streaming
- **Apache Spark 3.5 (Structured Streaming)** – Real-time processing
- **PostgreSQL** – Aggregated analytics storage
- **Docker & Docker Compose** – Container orchestration
- **Python** – Producer & Spark logic

## Prerequisites

- Docker and Docker Compose installed on your system
- Python 3.8+ (for running the producer locally)
- Git (for cloning the repository)

## Environment Variables

Create a `.env` file in the root directory with the following variables:

```env
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=stream_data
```

## Project Structure

```
realtime-kafka-spark-pipeline/
│
├── docker-compose.yml
├── producer.py
├── .gitignore
├── README.md
│
└── spark/
    ├── Dockerfile
    └── app/
        └── stream_app.py
```

## How to Run (Evaluator Instructions)

### 1. Clone the Repository

```bash
git clone <repository-url>
cd realtime-kafka-spark-pipeline
```

### 2. Start the Entire Pipeline

```bash
docker compose up --build
```

This will automatically start:

- Zookeeper
- Kafka
- PostgreSQL
- Spark Streaming Application

✅ No manual topic or table creation is required. All topics and tables are created automatically at runtime.

### 3. Start the Kafka Producer (New Terminal)

```bash
python producer.py
```

This sends real-time user activity events to Kafka.

## Verify Outputs

### Kafka Topics

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Expected topics:

- `user_activity`
- `enriched_activity`

### View Enriched Kafka Stream

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic enriched_activity \
  --from-beginning
```

### PostgreSQL Analytics Tables

```bash
docker exec -it db psql -U postgres -d stream_data
```

```sql
SELECT * FROM page_view_counts;
SELECT * FROM active_users;
```

Tables:

- `page_view_counts` – Page views per 1-minute window
- `active_users` – Active users per 5-minute sliding window

## Streaming Logic

### Page View Counts

- **Window**: 1 minute tumbling
- **Metric**: Page views per URL

### Active Users

- **Window**: 5 minutes sliding, 1 minute slide
- **Metric**: Approximate distinct users

### Enriched Events

- Adds `processing_time`
- Published back to Kafka

## Data Lake

- **Format**: Parquet
- **Partition**: `event_date`
- **Location**: `./data/lake` (mounted volume)

## Automatic Setup (Important)

- Kafka topics are auto-created
- PostgreSQL tables are created inside Spark at startup
- No manual SQL scripts required
- Fully reproducible on any machine

## Troubleshooting

If needed, clean restart:

```bash
docker compose down -v
docker compose up --build
```

## Final Notes

- Designed for evaluation & interviews
- Fully automated
- Uses best practices for real-time streaming
- Works consistently across systems

## Author

**Pavan Kumar Goli**  
Real-Time Data Engineering Project

