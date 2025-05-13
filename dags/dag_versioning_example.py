from airflow.sdk import dag, task, chain
from pendulum import datetime
from include.utils import say_hello


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    tags=["dag_versioning_example"],
    default_args={"retries": 2},
)
def dag_versioning_example():

    @task
    def t1():
        say_hello()

    @task(retry_delay=5)
    def t2():
        pass

    # @task
    # def t3():
    #     print("Hello!")

    chain(
        t1(),
        t2(),
        # t3(),
    )


dag_versioning_example()
