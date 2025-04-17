from airflow.sdk import dag, Asset, AssetWatcher, task
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
import os
from pendulum import datetime

SQS_QUEUE_URL = os.getenv(
    "SQS_QUEUE_URL", default="https://sqs.<region>.amazonaws.com/<account>/<queue>"
)

trigger = MessageQueueTrigger(queue=SQS_QUEUE_URL)
sqs_asset = Asset(
    "sqs_queue_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)]
)


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=[sqs_asset],
    tags=["syntax_examples"],
)
def event_driven_scheduling_example():

    @task 
    def my_task():
        pass 

    my_task() 


event_driven_scheduling_example()