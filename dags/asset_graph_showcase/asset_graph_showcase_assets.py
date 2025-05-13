from airflow.sdk import asset, Asset


# Bronze
@asset(schedule="0 0 * * *", tags=["asset_example_complex"])  # Every day at midnight
def raw_ingest_webhook_stream():
    pass


@asset(schedule="30 1 * * *", tags=["asset_example_complex"])  # Every day at 1:30 AM
def raw_ingest_api_data():
    pass


@asset(schedule="0 3 * * *", tags=["asset_example_complex"])  # Every day at 3:00 AM
def raw_ingest_database_dump():
    pass


@asset(schedule="15 4 * * *", tags=["asset_example_complex"])  # Every day at 4:15 AM
def raw_ingest_file_uploads():
    pass


@asset(schedule="45 5 * * *", tags=["asset_example_complex"])  # Every day at 5:45 AM
def raw_ingest_event_stream():
    pass


@asset(schedule="0 6 * * *", tags=["asset_example_complex"])  # Every day at 6:00 AM
def raw_ingest_sensor_data():
    pass


@asset(schedule="30 7 * * *", tags=["asset_example_complex"])  # Every day at 7:30 AM
def raw_ingest_third_party_apis():
    pass


# @asset(schedule="0 9 * * *", tags=["asset_example_complex"])  # Every day at 9:00 AM
# def raw_ingest_user_activity_logs():
#     pass


@asset(schedule="15 10 * * *", tags=["asset_example_complex"])  # Every day at 10:15 AM
def raw_ingest_transaction_records():
    pass


@asset(schedule="45 11 * * *", tags=["asset_example_complex"])  # Every day at 11:45 AM
def raw_ingest_email_data():
    pass


# Silver 1


@asset(schedule=[raw_ingest_webhook_stream], tags=["asset_example_complex"])
def silver_clean_webhook_stream():
    pass


@asset(schedule=[raw_ingest_api_data], tags=["asset_example_complex"])
def silver_normalize_api_data():
    pass


@asset(schedule=[raw_ingest_database_dump], tags=["asset_example_complex"])
def silver_format_database_dump():
    pass


@asset(schedule=[raw_ingest_file_uploads], tags=["asset_example_complex"])
def silver_validate_file_uploads():
    pass


@asset(schedule=[raw_ingest_event_stream], tags=["asset_example_complex"])
def silver_standardize_event_stream():
    pass


# Silver 2


@asset(
    schedule=[
        silver_clean_webhook_stream,
        silver_standardize_event_stream,
        silver_normalize_api_data,
    ],
    tags=["asset_example_complex"],
)
def silver_user_sessions():
    pass


# @asset(
#     schedule=[silver_format_database_dump, silver_clean_webhook_stream],
#     tags=["asset_example_complex"],
# )
# def silver_enriched_transactions():
#     pass


@asset(
    schedule=[
        silver_normalize_api_data,
        silver_validate_file_uploads,
        silver_user_sessions,
    ],
    tags=["asset_example_complex"],
)
def silver_user_profiles():
    pass


@asset(
    schedule=[silver_standardize_event_stream, raw_ingest_sensor_data],
    tags=["asset_example_complex"],
)
def silver_sensor_readings_calibrated():
    pass


@asset(schedule=[silver_normalize_api_data], tags=["asset_example_complex"])
def silver_api_health_checks():
    pass


@asset(
    schedule=[Asset("raw_ingest_user_activity_logs"), silver_user_sessions],
    tags=["asset_example_complex"],
)
def silver_behavioral_flags():
    pass


@asset(
    schedule=[Asset("silver_enriched_transactions"), raw_ingest_transaction_records],
    tags=["asset_example_complex"],
)
def silver_transaction_consistency_check():
    pass


@asset(
    schedule=[raw_ingest_email_data, silver_user_profiles],
    tags=["asset_example_complex"],
)
def silver_user_comms_summary():
    pass


# Gold


# @asset(
#     schedule=[silver_user_profiles, silver_user_sessions, silver_user_comms_summary],
#     tags=["asset_example_complex"],
# )
# def gold_user_360():
#     pass


@asset(
    schedule=[Asset("silver_enriched_transactions"), silver_transaction_consistency_check],
    tags=["asset_example_complex"],
)
def gold_financial_fact_table():
    pass


@asset(
    schedule=[silver_sensor_readings_calibrated, silver_behavioral_flags],
    tags=["asset_example_complex"],
)
def gold_device_behavior_model_input():
    pass


@asset(
    schedule=[silver_api_health_checks, silver_user_sessions],
    tags=["asset_example_complex"],
)
def gold_service_latency_summary():
    pass


@asset(
    schedule=[Asset("gold_user_360"), gold_financial_fact_table], tags=["asset_example_complex"]
)
def gold_kpi_dashboard_feed():
    pass
