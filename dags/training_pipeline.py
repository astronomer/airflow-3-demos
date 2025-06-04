"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://www.astronomer.io/docs/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

import polars as pl
from airflow.decorators import dag, task
from airflow.sdk import Param

# from airflow.sdk.definitions.asset import Asset
from pendulum import datetime

from include.mlops import (
    dump_pickle,
    fit_transform_dv,
    get_lin_reg_model,
    load_pickle,
    prepare_dataset,
)


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={
        "owner": "Kevin",
        # "retries": 1
    },
    tags=["mlops_zoomcamp"],
    params={
        "filename": Param(
            "/usr/local/airflow/include/data/input/yellow_tripdata_2023-03.parquet",
            type="string",
            title="Training parquet file",
            # minLength=10,
            # maxLength=30,
            description="This field is required. You can not submit without having text in here.",
            section="Typed parameters with Param object",
        )
    },
)
def training_pipeline():
    @task
    def get_filename_from_params(**context) -> str:
        import os

        filename = context["params"]["filename"]
        print(f"Training filename to process {filename}")

        if not filename or not os.path.exists(filename):
            raise FileNotFoundError(f"File not found: {filename}")

        df_raw = pl.read_parquet(filename)
        print(f"Training data size {df_raw.shape}")

        return filename

    @task
    def prepare_training_dataset(filename: str) -> tuple[str, str]:
        dataset_tag = "train"
        df_input, df_target = prepare_dataset(filename)

        input_path = f"/tmp/{dataset_tag}_input.parquet"
        df_input.write_parquet(input_path)

        target_path = f"/tmp/{dataset_tag}_target.parquet"
        df_target.write_parquet(target_path)

        print(f"Training data size after cleaning {df_input.shape}")

        return input_path, target_path

    # Define tasks
    # (
    #     # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
    #     outlets=[Asset("current_astronauts")]
    # )  # Define that this task updates the `current_astronauts` Dataset
    @task
    def transform_training_dataset(file_paths):
        input_path, target_path = file_paths
        df_input = pl.read_parquet(input_path)
        df_target = pl.read_parquet(target_path)

        x_train, dv = fit_transform_dv(df_input)
        y_train = df_target.to_numpy().ravel()

        train_path = "/tmp/train.pkl"
        dump_pickle((x_train, y_train), train_path)

        dv_path = "/tmp/dv.pkl"
        dump_pickle(dv, dv_path)

        return train_path, dv_path

    @task
    def train_lin_reg(file_paths):
        train_path, dv_path = file_paths
        x_train, y_train = load_pickle(train_path)

        lin_reg_model = get_lin_reg_model(x_train, y_train)

        print(f"LR model intercept = {lin_reg_model.intercept_}")

        model_path = "/tmp/lin_reg.pkl"
        dump_pickle(lin_reg_model, model_path)
        return model_path

    in_file_path = get_filename_from_params()
    mid_file_paths = prepare_training_dataset(in_file_path)
    out_file_paths = transform_training_dataset(mid_file_paths)
    model_path = train_lin_reg(out_file_paths)


# Instantiate the DAG
training_pipeline()
