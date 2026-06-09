import json
import uuid
import psycopg2
from confluent_kafka import Consumer
from datetime import datetime

# Configure
# This is the error budget
SLO_THRESHOLD = 0.10
BATCH_SIZE = 100  # evaluate quality every 100 records

# Connect to Postgres
conn = psycopg2.connect(
    host='127.0.0.1',
    port=5432,
    database='pipeline',
    user='postgres',
    password='postgres'
)
cursor = conn.cursor()

# Connect to Kafka (reads from BOTH clean and DLQ topics)
# This lets us count good vs bad records to calculate failure rate
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'slo-tracker-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['orders-clean', 'orders-dlq'])

# Tracking counters
total_records = 0
failed_records = 0
batch_id = str(uuid.uuid4())[:8]  

print(f"SLO Tracker started. Batch ID: {batch_id}")
print(f"SLO threshold: {SLO_THRESHOLD*100}% max failure rate")
print("─" * 50)

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            continue

        total_records += 1

        # If message came from DLQ topic it's a failed record
        if msg.topic() == 'orders-dlq':
            failed_records += 1

        # Every BATCH_SIZE records, calculate and log the failure rate
        if total_records % BATCH_SIZE == 0:
            failure_rate = failed_records / total_records
            slo_breached = failure_rate > SLO_THRESHOLD

            # Save to Postgres quality_metrics table
            cursor.execute(
                'INSERT INTO quality_metrics (batch_id, total_records, failed_records, failure_rate, slo_breached) VALUES (%s, %s, %s, %s, %s)',
                (batch_id, total_records, failed_records, round(failure_rate, 4), slo_breached)
            )
            conn.commit()

            # Print status
            status = "SLO BREACHED" if slo_breached else "SLO OK"
            print(f"{status} | batch={batch_id} | total={total_records} | failed={failed_records} | rate={failure_rate:.1%}")

            # If SLO is breached, print a warning
            if slo_breached:
                print(f"Failure rate {failure_rate:.1%} exceeds SLO of {SLO_THRESHOLD:.1%}")
                print(f"Action needed: investigate DLQ records")

except KeyboardInterrupt:
    print("\nSLO Tracker shutting down...")
    print(f"Final stats: {total_records} total, {failed_records} failed, {failed_records/max(total_records,1):.1%} failure rate")
finally:
    consumer.close()