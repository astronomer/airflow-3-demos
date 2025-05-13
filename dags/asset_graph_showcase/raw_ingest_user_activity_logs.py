from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Label, chain, chain_linear, dag, task, task_group
from pendulum import datetime


@dag(
    start_date=datetime(2025, 4, 1),
    schedule="@hourly",
    tags=["asset_example_complex"],
    params={"log_format": "json"},
)
def raw_ingest_user_activity_logs():

    start = EmptyOperator(task_id="start")

    # Dynamic extraction from multiple log drop points
    log_sources = ["cdn_edge", "web_app", "mobile_sdk"]
    extract_logs = BashOperator.partial(task_id="extract_logs").expand(
        bash_command=[f"echo pulling from {source}" for source in log_sources]
    )

    @task.branch
    def determine_log_parser(**context):
        fmt = context["params"]["log_format"]
        return "parse_json_logs" if fmt == "json" else "parse_csv_logs"

    # Optional parsing stage
    parse_json_logs = EmptyOperator(task_id="parse_json_logs")
    parse_csv_logs = EmptyOperator(task_id="parse_csv_logs")

    # Lightweight validation
    @task_group
    def validate_logs():
        check_schema = EmptyOperator(task_id="check_schema", trigger_rule="all_done")
        check_required_fields = EmptyOperator(task_id="check_required_fields")
        chain(check_schema, check_required_fields)

    validations = validate_logs()

    # Monitoring and metadata push
    @task_group
    def register_partitions():
        update_catalog = EmptyOperator(task_id="update_data_catalog")
        emit_metrics = EmptyOperator(task_id="emit_observability_metrics")
        chain(update_catalog, emit_metrics)

    partition_register = register_partitions()

    @task(trigger_rule="none_failed", outlets=[Asset("raw_ingest_user_activity_logs")])
    def complete_ingestion():
        pass

    end = complete_ingestion()

    # DAG wiring
    chain(
        start,
        extract_logs,
        determine_log_parser(),
        [parse_json_logs, parse_csv_logs],
        validations,
        partition_register,
        end,
    )

    chain(determine_log_parser(), Label("Structured JSON"), parse_json_logs)
    chain(determine_log_parser(), Label("Legacy CSV"), parse_csv_logs)


raw_ingest_user_activity_logs()
