from airflow.sdk import asset
from airflow.datasets import Dataset
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import json

probe_asset = Dataset("collect_probe_data")
counter_file = "/tmp/mars_probe_run_counter.txt"

def write_to_snowflake(data: dict):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    insert_sql = """
        INSERT INTO mars_probe_telemetry (temperature, radiation, version)
        VALUES (%s, %s, %s)
    """
    params = (data["temp"], data["radiation"], data["version"])
    hook.run(insert_sql, parameters=params)
    print("[mars_probe] Data inserted into Snowflake successfully.")

@asset(schedule="@daily", tags=["mars"])
def collect_probe_data():
    # Initialize or read the counter
    if not os.path.exists(counter_file):
        with open(counter_file, 'w') as f:
            f.write('0')

    with open(counter_file, 'r') as f:
        count = int(f.read())

    count += 1

    with open(counter_file, 'w') as f:
        f.write(str(count))

    print(f"[mars_probe] Run count: {count}")

    # Fail every 4th run
    if count % 4 == 0:
        print(f"[mars_probe] CRITICAL FAILURE SIMULATION on run {count} 🚨")
        raise Exception(f"[mars_probe] Simulated failure on run {count}")

    # Generate telemetry data
    data = {
        "temp": -66,
        "radiation": "medium",
        "version": "v2"
    }

    # Write to Dataset file
    with open(probe_asset.name, 'w') as f:
        json.dump(data, f)

    # Insert into Snowflake
    write_to_snowflake(data)

    print("[mars_probe] Collecting enhanced telemetry from Mars probe complete.")

    # ➡️ Return the dataset object here
    return probe_asset

@asset(schedule=[probe_asset], tags=["mars"])
def preprocess_probe_data():
    with open(probe_asset.name, 'r') as f:
        content = f.read()
    print(f"[mars_probe] Preprocessing telemetry on-probe with data: {content}")