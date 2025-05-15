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


def delete_dagrun(
    dag_id,
    dag_run_id,
):

    token = get_jwt_token()

    if token:
        url = f"{HOST}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.delete(url,  headers=headers)

        print(response.status_code)
        # print(response.json())


def delete_dag(
        dag_id,
):
    token = get_jwt_token()

    if token:
        url = f"{HOST}/api/v2/dags/{dag_id}"
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.delete(url,  headers=headers)

        print(response.status_code)
        # print(response.json())


if __name__ == "__main__":
    # delete_dagrun(
    #     dag_id="dag_in_a_gitdagbundle",
    #     dag_run_id="manual__2025-05-15T11:23:26.646742+00:00",
    # )

    delete_dag(
        dag_id="dag_in_a_gitdagbundle",
    )
