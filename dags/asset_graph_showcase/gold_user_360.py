from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, Label, chain, chain_linear, dag, task, task_group
from pendulum import datetime


@dag(
    start_date=datetime(2025, 4, 1),
    schedule=[
        Asset("silver_user_profiles"),
        Asset("silver_user_sessions"),
        Asset("silver_user_comms_summary"),
    ],
    tags=["asset_example_complex"],
    params={"region": ["NA", "EU", "APAC"]},
)
def gold_user_360():

    start = EmptyOperator(task_id="start")

    # Extract Silver layer user-related datasets
    silver_sources = [
        "silver_user_profiles",
        "silver_user_sessions",
        "silver_user_comms_summary",
    ]
    extract_silver = BashOperator.partial(task_id="extract_silver_inputs").expand(
        bash_command=[f"echo extracting {src}" for src in silver_sources]
    )

    @task_group
    def profile_stitching():
        deduplicate = EmptyOperator(task_id="deduplicate_users")
        session_linking = EmptyOperator(task_id="link_sessions_to_profiles")
        comms_merge = EmptyOperator(task_id="merge_communication_channels")
        chain(deduplicate, session_linking, comms_merge)

    stitched_profiles = profile_stitching()

    @task_group
    def profile_enrichment():
        segment_users = EmptyOperator(task_id="segment_users")
        compute_lifetime_value = EmptyOperator(task_id="compute_ltv")
        assign_tags = EmptyOperator(task_id="assign_user_tags")
        chain(segment_users, compute_lifetime_value, assign_tags)

    enriched_profiles = profile_enrichment()

    @task_group
    def scoring():
        churn_score = EmptyOperator(task_id="predict_churn")
        engagement_score = EmptyOperator(task_id="score_engagement")
        risk_score = EmptyOperator(task_id="calculate_fraud_risk")
        chain(churn_score, engagement_score, risk_score)

    ml_scoring = scoring()

    @task_group
    def reporting_by_region(region):
        generate = EmptyOperator(task_id="generate_profile_report")
        publish = EmptyOperator(task_id="publish_report")
        chain(generate, publish)

    regional_reports = reporting_by_region.expand(region=["NA", "EU", "APAC"])

    @task_group
    def external_exports():
        to_snowflake = EmptyOperator(task_id="export_to_snowflake")
        to_crm = EmptyOperator(task_id="push_to_crm")
        to_segment = EmptyOperator(task_id="sync_to_segment")
        chain(to_snowflake, to_crm, to_segment)

    export_targets = external_exports()

    @task(
        trigger_rule="none_failed",
        outlets=[
            Asset("gold_user_360"),
            Asset("gold_user_360_report"),
            Asset("gold_user_360_segments"),
            Asset("gold_user_360_scores"),
            Asset("gold_user_360_exports"),
        ],
    )
    def finalize_gold_user_360():
        pass

    end = finalize_gold_user_360()

    # DAG Wiring
    chain(
        start,
        extract_silver,
        stitched_profiles,
        enriched_profiles,
        [ml_scoring, regional_reports],
        export_targets,
        end,
    )

    # Add explanatory labels
    chain(stitched_profiles, Label("Behavioral + ID join"), enriched_profiles)
    chain(enriched_profiles, Label("Enriched with segments & LTV"), ml_scoring)
    chain(ml_scoring, Label("ML-driven scoring"), export_targets)


gold_user_360()
