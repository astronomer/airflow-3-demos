from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Label, chain, chain_linear, dag, task, task_group
from pendulum import datetime


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=[
        Asset("silver_format_database_dump"),
        Asset("silver_clean_webhook_stream"),
    ],
    tags=["asset_example_complex"],
    params={"force_full_reload": False},
)
def silver_enriched_transactions():

    start = EmptyOperator(task_id="start")

    # Extract phase (dynamic)
    transaction_sources = ["banking_core", "payments_gateway", "rewards_api"]
    extract_transactions = BashOperator.partial(task_id="extract_transactions").expand(
        bash_command=[f"echo extracting from {src}" for src in transaction_sources]
    )

    # Branch: full vs incremental transform
    @task.branch
    def choose_transform_mode(**context):
        return (
            "transform_full"
            if context["params"]["force_full_reload"]
            else "transform_incremental"
        )

    transform_full = EmptyOperator(task_id="transform_full")
    transform_incremental = EmptyOperator(task_id="transform_incremental")

    load_silver = EmptyOperator(task_id="load_silver_enriched_transactions")

    # Validation Task Group
    @task_group
    def validate_enriched_data():
        schema_check = EmptyOperator(task_id="schema_check", trigger_rule="all_done")
        row_count_check = EmptyOperator(task_id="row_count_check")
        duplicate_check = EmptyOperator(task_id="duplicate_check")
        chain(schema_check, row_count_check, duplicate_check)

    validations = validate_enriched_data()

    # Reporting Task Group (mapped by region)
    @task_group
    def regional_reporting(region):
        prepare = EmptyOperator(task_id="prepare_report")
        publish = EmptyOperator(task_id="publish_report")
        chain(prepare, publish)

    reporting = regional_reporting.expand(region=["NA", "EU", "APAC"])

    # MLops Task Group
    @task_group
    def fraud_model_training():
        setup = EmptyOperator(task_id="setup_cluster", trigger_rule="all_done")
        train = EmptyOperator(task_id="train_model")
        teardown = EmptyOperator(task_id="teardown_cluster")
        chain(setup, train, teardown)
        teardown.as_teardown(setups=setup)

    mlops = fraud_model_training()

    @task(trigger_rule="none_failed", outlets=[Asset("silver_enriched_transactions")])
    def finalize_dataset():
        pass

    end = finalize_dataset()

    # DAG wiring
    chain(
        start,
        extract_transactions,
        choose_transform_mode(),
        [transform_full, transform_incremental],
        load_silver,
        [validations, reporting],
        end,
    )

    chain_linear([transform_full, transform_incremental], [load_silver])

    chain(load_silver, mlops, end)

    chain(choose_transform_mode(), Label("Full Reload"), transform_full)
    chain(choose_transform_mode(), Label("Delta Only"), transform_incremental)


silver_enriched_transactions()
