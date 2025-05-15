from airflow.sdk import dag, task, chain
from pendulum import datetime
from include.utils import say_hello


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    tags=["dag_versioning_example", "webinar_example"],
    default_args={"retries": 4},
)
def dag_versioning_example():

    @task
    def task_1():
        say_hello()

    @task(retry_delay=10)
    def task_2_changed_code():
        print("Hello!")
        print("If you make a task code change that wont change the DAG version, unless you also change the task_id!")

    @task
    def added_a_task():
        print("Hello!")


    # @task
    # def task_removed_in_later_version():
    #     print("Hello!")

    chain(
        added_a_task(),
        task_1(),
        task_2_changed_code(),
        
    )


dag_versioning_example()
