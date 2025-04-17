"""
Code structure of the asset-oriented approach

Simple ETL pipeline created using 3 assets.
Each asset defines one dag with one task that materializes the asset.
"""

from airflow.sdk import asset


@asset(schedule="@daily", tags=["syntax_examples", "asset_example_passing_data"])
def extracted_data():
    return {"a": 1, "b": 2, "c": 3}


@asset(schedule=extracted_data, tags=["syntax_examples", "asset_example_passing_data"])
def transformed_data(context):

    extracted_data = context["ti"].xcom_pull(
        dag_id="extracted_data",
        task_ids=["extracted_data"],
        key="return_value",
        include_prior_dates=True,
    )

    sum_data = sum(extracted_data.values())
    return {"sum": sum_data}


@asset(
    schedule=transformed_data, tags=["syntax_examples", "asset_example_passing_data"]
)
def loaded_data(context):

    transformed_data = context["ti"].xcom_pull(
        dag_id="transformed_data",
        task_ids=["transformed_data"],
        key="return_value",
        include_prior_dates=True,
    )

    print(f"Loaded data: {transformed_data}")
