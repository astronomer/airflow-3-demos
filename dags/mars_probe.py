from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import asset, Asset, AssetWatcher
from airflow.datasets import Dataset
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import json

# Dataset reference for chaining
probe_asset = Dataset("collect_probe_data")
counter_file = "/tmp/mars_probe_run_counter.txt"

# Define SQS-based trigger asset (drives collect_probe_data)
SQS_QUEUE = os.getenv(
    "SQS_QUEUE", "https://sqs.us-east-1.amazonaws.com/001177193081/MarsTelemetry"
)
trigger = MessageQueueTrigger(queue=SQS_QUEUE)

sqs_queue_asset = Asset(
    "sqs_probe_trigger",
    watchers=[AssetWatcher(name="probe_watcher", trigger=trigger)]
)

# Asset 1: Collect data from probe (triggered by SQS)
@asset(name="collect_probe_data", schedule=[sqs_queue_asset])
def collect_probe_data():
    # Run counter
    if not os.path.exists(counter_file):
        with open(counter_file, 'w') as f:
            f.write('0')
    with open(counter_file, 'r') as f:
        count = int(f.read())
    count += 1
    with open(counter_file, 'w') as f:
        f.write(str(count))
    print(f"[mars_probe] Run count: {count}")

    # Simulate failure every 4th run
    if count % 4 == 0:
        print(f"[mars_probe] CRITICAL FAILURE SIMULATION on run {count} 🚨")
        raise Exception(f"[mars_probe] Simulated failure on run {count}")

    # Simulate telemetry
    data = {
        "temp": -66,
        "radiation": "medium",
        "version": "v2"
    }

    # Write to dataset
    with open(probe_asset.name, 'w') as f:
        json.dump(data, f)

    # Connect to Snowflake
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # Create table if it doesn't exist
    create_sql = """
        CREATE TABLE IF NOT EXISTS mars_probe_telemetry (
            id INT AUTOINCREMENT PRIMARY KEY,
            temperature FLOAT,
            radiation STRING,
            version STRING,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    hook.run(create_sql)
    print("[mars_probe] Ensured mars_probe_telemetry table exists.")

    # Insert telemetry
    insert_sql = """
        INSERT INTO mars_probe_telemetry (temperature, radiation, version)
        VALUES (%s, %s, %s)
    """
    params = (data["temp"], data["radiation"], data["version"])
    hook.run(insert_sql, parameters=params)
    print("[mars_probe] Data inserted into Snowflake successfully.")

# Asset 2: Preprocess data (triggered by Dataset)
@asset(name="preprocess_probe_data", schedule=[probe_asset])
def preprocess_probe_data():
    with open(probe_asset.name, 'r') as f:
        content = f.read()
    print(f"[mars_probe] Preprocessing telemetry on-probe with data: {content}")