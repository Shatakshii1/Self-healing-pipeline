import json
import psycopg2
from confluent_kafka import Consumer, Producer
from datetime import datetime

# Connect to Kafka consumer
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'orders-consumer-group-4',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['orders-raw'])

# Producer to forward messages to clean or DLQ topics
producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

# Connect to Postgres
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='pipeline',
    user='postgres',
    password='postgres'
)
cursor = conn.cursor()

def validate_record(record):
    errors = []
    if not record.get('order_id'):
        errors.append('missing order_id')
    if not record.get('customer_id'):
        errors.append('missing customer_id')
    payment = record.get('payment_value')
    if payment is None:
        errors.append('missing payment_value')
    elif float(payment) <= 0:
        errors.append('payment_value must be positive')
    timestamp = record.get('order_purchase_timestamp')
    if timestamp:
        try:
            datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S')
        except ValueError:
            errors.append('invalid order_purchase_timestamp')
    return errors

# Create DLQ log table in Postgres
cursor.execute('''
    CREATE TABLE IF NOT EXISTS dlq_log (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(255),
        error_reasons TEXT,
        raw_record TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')
conn.commit()

print("Consumer started. Listening for messages...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        record = json.loads(msg.value().decode('utf-8'))
        errors = validate_record(record)

        if errors:
            record['dlq_reason'] = errors
            producer.produce('orders-dlq', value=json.dumps(record).encode('utf-8'))
            producer.poll(0)
            cursor.execute(
                'INSERT INTO dlq_log (order_id, error_reasons, raw_record) VALUES (%s, %s, %s)',
                (
                    record.get('order_id'),
                    ', '.join(errors),
                    json.dumps(record)
                )
            )
            conn.commit()
            print(f"BAD record routed to DLQ: {errors}")
        else:
            producer.produce('orders-clean', value=json.dumps(record).encode('utf-8'))
            producer.poll(0)
            print(f"Clean record: order_id={record.get('order_id')}")

except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()