# Self-Healing Data Pipeline

A real-time data pipeline that automatically detects and recovers from failures.

Built with **Apache Kafka**, **Confluent Schema Registry**, **PostgreSQL**, and **Python**.
---

This pipeline detects its own failures and fixes them automatically:

| Problem | What happens | How it heals |
|---|---|---|
| Corrupt/missing data | Record fails validation | Routed to Dead Letter Queue, logged to Postgres, reprocessable |
| Error rate too high | Failure rate exceeds 10% SLO | Automatic alert + audit trail in Postgres |
| Schema change | New field added upstream | Detected automatically, migrated without downtime |

---

## Architecture

```
E-Commerce Orders (Kaggle Dataset)
│
▼
[Kafka: orders-raw]
│
▼
Consumer (Validator)
┌──────┴──────┐
▼             ▼
[orders-clean] [orders-dlq]
│
▼
Postgres (dlq_log)
│
▼
SLO Tracker (quality_metrics)
```

---

## Self-Healing Mechanisms

### Mechanism 1 — Dead Letter Queue
Bad records (null order IDs, negative payments, invalid timestamps) are automatically detected and routed to `orders-dlq` instead of crashing the pipeline. Every bad record is logged to Postgres with the exact error reason and timestamp for audit and reprocessing.

### Mechanism 2 — Schema Evolution
When upstream data adds new fields, the pipeline detects the change using Confluent Schema Registry, automatically migrates to the new schema, and logs the migration(all without downtime).

### Mechanism 3 — Data Quality SLOs
The pipeline tracks a real error budget: failure rate must stay below 10%. Every 100 records, metrics are written to Postgres. When the SLO is breached, the system alerts automatically with batch ID, failure rate, and record counts.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Apache Kafka | Event streaming backbone |
| Confluent Schema Registry | Schema versioning and evolution |
| PostgreSQL | DLQ metadata + quality metrics storage |
| Python (confluent-kafka) | Producer, consumer, validation logic |
| Docker | Local infrastructure |

---

## Challenges Solved

- **Port conflict debugging**: Resolved authentication failures caused by two PostgreSQL instances (Docker + Windows) competing on port 5432
- **Docker networking**: Configured dual Kafka listeners (EXTERNAL for host, INTERNAL for containers) so Python scripts and Schema Registry can both reach Kafka
- **Schema Registry integration**: Got Confluent Schema Registry communicating with Kafka across Docker network boundaries

---

## Dataset

[Ecommerce Order & Supply Chain Dataset](https://www.kaggle.com/datasets/bytadit/ecommerce-order-dataset) from Kaggle.

Download and place CSV files in `data/train/` to run locally.

---

## How to Run

### Prerequisites
- Docker Desktop
- Python 3.11+

### 1. Start infrastructure
```bash
docker compose up -d
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Create Kafka topics
```bash
docker exec -it kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic orders-raw --partitions 3 --replication-factor 1
docker exec -it kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic orders-clean --partitions 3 --replication-factor 1
docker exec -it kafka /usr/bin/kafka-topics --create --bootstrap-server localhost:9092 --topic orders-dlq --partitions 3 --replication-factor 1
```

### 4. Run the pipeline (4 terminals)
```bash
# Terminal 1: consume and validate
python consumer/consumer.py

# Terminal 2: track SLO metrics
python monitoring/slo_tracker.py

# Terminal 3: stream data
python producer/producer.py

# Terminal 4: schema evolution demo
python monitoring/schema_evolution.py
```

---

## What I Would Add Next

- **Adaptive backpressure**: Spark Structured Streaming to auto-scale consumers based on Kafka lag
- **Predictive anomaly detection**: lightweight ML model trained on pipeline metrics to detect degradation before failure
- **Grafana dashboard**: real-time visualization of SLO error budget and DLQ depth
- **Chaos testing suite**: automated failure injection with documented recovery times
