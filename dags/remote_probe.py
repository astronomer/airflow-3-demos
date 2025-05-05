from airflow.sdk import dag, task
from datetime import datetime
import time
import json
import random
import boto3
import os
import botocore.exceptions
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

@dag(start_date=datetime(2024, 12, 1), schedule="@daily", tags=['remote'])
def remote_probe():

    @task()
    def authenticate_v2():
        print("[remote] Authenticating with probe...")
        time.sleep(10)
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

    @task()
    def collect_response():
        print("[remote] Waiting for probe response...")
        time.sleep(8)
        response = {
            "temperature": random.randint(-80, -60),
            "radiation": random.choice(["low", "medium", "high"]),
            "timestamp": datetime.utcnow().isoformat(),
            "raw_payload": "0xffe34abc19"
        }
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

        try:
            # Load AWS credentials from Airflow's aws_default connection
            aws_hook = AwsBaseHook(aws_conn_id="aws_default")
            credentials = aws_hook.get_credentials()

            # Manually build the boto3 client with region and credentials
            sqs = boto3.client(
                "sqs",
                region_name="us-east-1",
                aws_access_key_id=credentials.access_key,
                aws_secret_access_key=credentials.secret_key,
                aws_session_token=credentials.token,
            )

            # Get queue URL from environment
            sqs_queue_url = os.environ.get(
                "SQS_QUEUE_URL",
                "https://sqs.us-east-1.amazonaws.com/001177193081/MarsTelemetry"
            )

            message_body = json.dumps(metadata)
            response = sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=message_body)
            print(f"[remote] Sent SQS message ID: {response['MessageId']}")

        except botocore.exceptions.BotoCoreError as e:
            print(f"[remote] Failed to send SQS message: {str(e)}")

    # Define DAG flow
    auth_token = authenticate_v2()
    check = check_probe_status(auth_token)
    transmit = transmit_command()
    response = collect_response()
    enriched = enrich_metadata(response)
    log_completion(enriched)

remote_probe()