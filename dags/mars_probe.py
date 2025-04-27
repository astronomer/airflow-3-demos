from airflow.sdk import asset
from airflow.datasets import Dataset
import os

probe_asset = Dataset("collect_probe_data")
counter_file = "/tmp/mars_probe_run_counter.txt"

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
        raise Exception(f"[mars_probe] Simulated failure on run {count}")

    # Otherwise proceed normally
    with open(probe_asset.name, 'w') as f:
        f.write('{"temp": -66, "radiation": "medium", "version": "v2"}')
    print("[mars_probe] Collecting enhanced telemetry from Mars probe and writing to dataset...")

@asset(schedule=collect_probe_data, tags=["mars"])
def preprocess_probe_data():
    with open(probe_asset.name, 'r') as f:
        content = f.read()
    print(f"[mars_probe] Preprocessing telemetry on-probe with data: {content}")