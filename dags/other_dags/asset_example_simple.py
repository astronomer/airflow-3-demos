"""
Code structure of the asset-oriented approach - simple example
"""

from airflow.sdk import asset, dag, task, chain, Asset
from airflow.providers.standard.operators.bash import BashOperator

@asset(schedule="@daily", tags=["asset_example_simple"])
def my_asset_1():
    pass


@asset(schedule=my_asset_1, tags=["asset_example_simple"])
def my_asset_2():
    pass


@asset(schedule=my_asset_2, tags=["asset_example_simple"])
def my_asset_3():
    pass

@asset(schedule=my_asset_2, tags=["asset_example_simple"])
def my_asset_4():
    pass


@dag(
    schedule=[Asset("my_asset_3")],
    tags=["asset_example_simple"],
)
def my_dag():

    @task
    def my_task_1():
        pass

    _my_task_2 = BashOperator(
        task_id="my_task_2",
        bash_command="echo 'Hello from my_task_2'",
    )

    chain(
        my_task_1(),
        _my_task_2,
    )


my_dag()
