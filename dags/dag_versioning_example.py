from airflow.sdk import dag, task, chain
from pendulum import datetime
from include.utils import say_hello


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    tags=["dag_versioning_example"],
)
def dag_versioning_example():

    @task
    def t1():
        say_hello()

    @task
    def t2():
        pass

    chain(
        t1(),
        t2(),
    )


dag_versioning_example()
