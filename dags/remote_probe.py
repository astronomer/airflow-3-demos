from airflow.sdk import Asset, dag, task
from datetime import datetime
import time
import json
import random

probe_response_asset = Asset("/tmp/remote_probe_response.json")

@dag(start_date=datetime(2024, 12, 1), schedule=None, tags=['remote'])
def remote_probe():

    @task()
    def authenticate():
        print("[remote] Authenticating with probe...")
        time.sleep(3)
        token = "fake-auth-token-123"
        print(f"[remote] Authentication successful. Token: {token}")
        return token

    @task()
    def check_probe_status(token: str):
        print(f"[remote] Using token {token} to check probe status...")
        time.sleep(5)
        print("[remote] Probe systems nominal.")
        return True

    @task()
    def transmit_command():
        print("[remote] Sending data collection command to probe...")
        time.sleep(5)
        print("[remote] Command transmission complete.")
        return True

    @task(outlets=[probe_response_asset])
    def collect_response():
        print("[remote] Waiting for probe response...")
        time.sleep(8)
        response = {
            "temperature": random.randint(-80, -60),
            "radiation": random.choice(["low", "medium", "high"]),
            "timestamp": datetime.utcnow().isoformat(),
            "raw_payload": "0xffe34abc19"
        }
        with open(probe_response_asset.uri, 'w') as f:
            json.dump(response, f)
        print(f"[remote] Probe responded with: {response}")
        return response

    @task()
    def enrich_metadata(response):
        print("[remote] Enriching response metadata...")
        enriched = {
            "temperature_c": response["temperature"],
            "radiation_level": response["radiation"],
            "iso_timestamp": response["timestamp"],
            "payload_checksum": hash(response["raw_payload"]),
            "probe_id": "MARS_PROBE_7"
        }
        print(f"[remote] Enriched response: {enriched}")
        return enriched

    @task()
    def log_completion(metadata):
        print("[remote] Remote probe communication cycle complete.")
        print(f"[remote] Final metadata logged: {metadata}")

    # Define DAG flow
    auth_token = authenticate()
    check = check_probe_status(auth_token)
    transmit = transmit_command()
    response = collect_response()
    enriched = enrich_metadata(response)
    log_completion(enriched)

remote_probe()