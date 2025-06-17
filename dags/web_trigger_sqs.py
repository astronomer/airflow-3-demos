from airflow.sdk import dag, task, chain
from pendulum import datetime
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
import os

QUEUE_URL = os.getenv(
    "SQS_QUEUE_URL", default="https://sqs.<region>.amazonaws.com/<account>/<queue>"
)


@dag(start_date=datetime(2025, 5, 1), schedule=None, params={"num_messages": 1}, tags=["event_driven_scheduling_example"])
def web_trigger_sqs():

    @task
    def send_message_to_sqs(queue_url: str, aws_conn_id: str = "aws_default", **context):
        import json

        num_messages = context["params"]["num_messages"]

        for i in range(num_messages):
            message_body = {"hello": "world"}

            sqs_hook = SqsHook(aws_conn_id=aws_conn_id)

            response = sqs_hook.send_message(
                queue_url=queue_url, message_body=json.dumps(message_body)
            )

            print(f"Message sent to SQS. Response: {response}")

        

    chain(send_message_to_sqs(queue_url=QUEUE_URL))


web_trigger_sqs()
