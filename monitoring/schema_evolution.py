import json
import requests
from datetime import datetime

# Schema registory connection
SCHEMA_REGISTRY_URL = 'http://localhost:8081'
SUBJECT = 'orders-value'

# Current schema the pipeline expects
v1_schema = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": ["null", "string"]},
        {"name": "customer_id", "type": ["null", "string"]},
        {"name": "payment_value", "type": ["null", "double"]},
        {"name": "order_purchase_timestamp", "type": ["null", "string"]},
        {"name": "order_status", "type": ["null", "string"]}
    ]
}

# This simulates a real schema change in production
v2_schema = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": ["null", "string"]},
        {"name": "customer_id", "type": ["null", "string"]},
        {"name": "payment_value", "type": ["null", "double"]},
        {"name": "order_purchase_timestamp", "type": ["null", "string"]},
        {"name": "order_status", "type": ["null", "string"]},
        {"name": "discount_code", "type": ["null", "string"], "default": None}
    ]
}

# Register Schema Function 
def register_schema(subject, schema):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
    payload = {"schema": json.dumps(schema)}
    response = requests.post(url, json=payload, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    return response.json()

# Get latest schema function
def get_latest_schema(subject):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    if response.status_code == 404:
        return None
    return response.json()

# Detect schema changes
def detect_changes(old_schema, new_schema):
    old_fields = {f['name'] for f in old_schema['fields']}
    new_fields = {f['name'] for f in new_schema['fields']}
    added = new_fields - old_fields
    removed = old_fields - new_fields
    return added, removed

# Registers v1, then evolve to v2
print("Schema Evolution Demo")

# Step 1: register v1 schema
print("\n[1] Registering v1 schema...")
result = register_schema(SUBJECT, v1_schema)
print(f"    Registered with ID: {result.get('id')}")

# Step 2: check what's currently registered
print("\n[2] Current schema in registry:")
latest = get_latest_schema(SUBJECT)
current = json.loads(latest['schema'])
print(f"    Fields: {[f['name'] for f in current['fields']]}")

# Step 3: detect changes between v1 and v2
print("\n[3] Detecting schema changes...")
added, removed = detect_changes(v1_schema, v2_schema)
if added:
    print(f"    NEW fields detected: {added}")
if removed:
    print(f"    REMOVED fields detected: {removed}")
if not added and not removed:
    print("    No changes detected")

# Step 4: register v2 schema (migration)
print("\n[4] Registering v2 schema (migration)...")
result = register_schema(SUBJECT, v2_schema)
print(f"    Migrated to schema ID: {result.get('id')}")

# Step 5: log the migration
migration_log = {
    "timestamp": datetime.now().isoformat(),
    "subject": SUBJECT,
    "fields_added": list(added),
    "fields_removed": list(removed),
    "action": "auto-migrated to v2"
}
with open('monitoring/schema_migration_log.jsonl', 'a') as f:
    f.write(json.dumps(migration_log) + '\n')

print("\n[5] Migration logged to monitoring/schema_migration_log.jsonl")
print("\nDone! Pipeline adapted to new schema without downtime.")
