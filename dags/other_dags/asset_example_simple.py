"""
Code structure of the asset-oriented approach - simple example
"""

from airflow.sdk import asset


@asset(schedule="@daily", tags=["asset_example_simple"])
def my_asset_1():
    pass


@asset(schedule=my_asset_1, tags=["asset_example_simple"])
def my_asset_2():
    pass


@asset(schedule=my_asset_2, tags=["asset_example_simple"])
def my_asset_3():
    pass
