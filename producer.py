import json
import random
import pandas as pd
from confluent_kafka import Producer

# Connect to Kafka and create a producer
producer = Producer({
    'bootstrap.servers': 'localhost:9092'
})

# Loads the data and reads both CSV files into pandas DataFrames
print("Loading data...")
orders = pd.read_csv('data/train/df_Orders.csv')
payments = pd.read_csv('data/train/df_Payments.csv')

# Join orders with payments on order_id so each event has payment info
df = pd.merge(orders, payments, on='order_id', how='left')
print(f"Loaded {len(df)} records. Starting to stream...")

# This function randomly corrupts a record to simulate real-world bad data
# 20% of records will be corrupted
def corrupt_record(record):
    corruption_type = random.choice([
        'null_order_id',
        'null_payment',
        'negative_payment',
        'bad_timestamp',
        'null_customer'
    ])
    if corruption_type == 'null_order_id':
        record['order_id'] = None
    elif corruption_type == 'null_payment':
        record['payment_value'] = None
    elif corruption_type == 'negative_payment':
        record['payment_value'] = -999.99
    elif corruption_type == 'bad_timestamp':
        record['order_purchase_timestamp'] = 'NOT_A_DATE'
    elif corruption_type == 'null_customer':
        record['customer_id'] = None
    record['corruption_type'] = corruption_type
    return record

# Stream records into Kafka
for index, row in df.iterrows():
    record = row.where(pd.notna(row), None).to_dict()

    if random.random() < 0.20:
        record = corrupt_record(record)
    else:
        record['corruption_type'] = None

    producer.produce('orders-raw', value=json.dumps(record).encode('utf-8'))

    if index % 100 == 0:
        print(f"Sent {index} records... (last: order_id={record.get('order_id')})")

    producer.poll(0)

producer.flush()
print("Done! All records streamed to Kafka.")