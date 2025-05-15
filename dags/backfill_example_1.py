from airflow.sdk import dag, task, chain
from pendulum import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    tags=["backfill_example", "webinar_example"],
)
def backfill_example_1():
    """
    Backfill example 1
    """

    @task
    def t1():
        print("Task 1")

    @task
    def t2():
        print("Task 2")

    @task
    def t3():
        print("Task 3")

    @task
    def t4():
        print("Task 4")

    @task
    def t5():
        print("Task 5")

    chain(
        t1(),
        t2(),
        t3(),
        t4(),
        t5(),
    )


backfill_example_1()
