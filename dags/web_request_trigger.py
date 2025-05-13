from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
import os

@task
def send_message_to_sqs(queue_url: str, message_body: str, aws_conn_id: str = "aws_default"):
    """
    Task to send a message to an AWS SQS queue using the SqsHook.
    """
    # Initialize the SQS Hook with the specified AWS connection ID
    sqs_hook = SqsHook(aws_conn_id=aws_conn_id)
    
    # Send the message to the specified SQS queue
    response = sqs_hook.send_message(queue_url=queue_url, message_body=message_body)
    
    # Log the response for debugging purposes
    print(f"Message sent to SQS. Response: {response}")

@dag(
    dag_id='web_request_trigger',
)
def send_message_to_sqs_dag():
    """
    DAG to send a predefined message to an AWS SQS queue.
    """
    # Define the SQS queue URL and message body
    queue_url = os.getenv(
    "SQS_QUEUE_URL", default="https://sqs.<region>.amazonaws.com/<account>/<queue>"
)

    message_body = '''{
    "id": 300,
    "name": "Kenten",
    "location": "Seattle",
    "motivation": "Finding my way.",
    "favorite_sci_fi_character": "Spock (Star Trek)"
    }'''
    
    # Call the task to send the message
    send_message_to_sqs(queue_url=queue_url, message_body=message_body)

send_message_to_sqs_dag()