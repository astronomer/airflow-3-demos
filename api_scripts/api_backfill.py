# Airflow API Backfill Example
# Airflow REST API documentation: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

import requests

USERNAME = "admin"
PASSWORD = "admin"
HOST = "http://localhost:8080/"  # To learn how to send API requests to Airflow running on Astro see: https://www.astronomer.io/docs/astro/airflow-api/


def get_jwt_token():
    token_url = f"{HOST}/auth/token"
    payload = {"username": USERNAME, "password": PASSWORD}
    headers = {"Content-Type": "application/json"}
    response = requests.post(token_url, json=payload, headers=headers)

    token = response.json().get("access_token")
    return token


def backfill_dag(
    dag_id,
    from_date,
    to_date,
    run_backwards=False,
    dag_run_conf={},
    reprocess_behavior="none",  # can be "failed" (Missing runs will be filled in and errored runs will be reprocessed), "completed" (All runs will be reprocessed) or "none" (Missing Runs will be filled in)
    max_active_runs=5,
):
    event_payload = {
        "dag_id": dag_id,
        "from_date": from_date,
        "to_date": to_date,
        "run_backwards": run_backwards,
        "dag_run_conf": dag_run_conf,
        "reprocess_behavior": reprocess_behavior,
        "max_active_runs": max_active_runs,
    }
    token = get_jwt_token()

    if token:
        url = f"{HOST}/api/v2/backfills"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(url, json=event_payload, headers=headers)

        print(response.status_code)
        print(response.json())


if __name__ == "__main__":
    backfill_dag(
        dag_id="backfill_example_1",
        from_date="2025-01-01",
        to_date="2025-01-04",
        reprocess_behavior="none",  # can be "failed" (Missing runs will be filled in and errored runs will be reprocessed), "completed" (All runs will be reprocessed) or "none" (Missing Runs will be filled in)
    )
