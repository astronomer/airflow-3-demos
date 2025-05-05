from airflow.sdk import dag, task, Asset
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from transformers import pipeline
import json

@dag(
    start_date=datetime(2024, 12, 1),
    schedule=[Asset("confirmed_anomaly")],
    tags=["mars", "genai", "report"]
)
def anomaly_report_generator():

    @task()
    def load_anomaly():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        sql = """
            SELECT temperature, radiation, version, collected_at
            FROM mars_probe_telemetry
            WHERE temperature IS NOT NULL AND radiation IS NOT NULL
            ORDER BY collected_at DESC
            LIMIT 1
        """
        record = hook.get_first(sql)
        if not record:
            raise Exception("[report] No telemetry data found.")

        anomaly = {
            "temperature": record[0],
            "radiation": record[1],
            "version": record[2],
            "timestamp": record[3].isoformat()
        }

        print(f"[report] Loaded anomaly data: {anomaly}")
        return json.dumps(anomaly)

    @task()
    def generate_report(raw_json):
        parsed = json.loads(raw_json)

        prompt = f"""
        Mars anomaly report generation:

        Telemetry snapshot:
        - Temperature: {parsed['temperature']}°C
        - Radiation level: {parsed['radiation']}
        - Timestamp: {parsed['timestamp']}
        - Software Version: {parsed['version']}

        Create a concise anomaly report that:
        1. Describes the issue in technical terms.
        2. Estimates severity level.
        3. Recommends next steps for the ground team.
        4. Sounds like it came from a mission control expert.
        """

        gen = pipeline("text-generation", model="tiiuae/falcon-rw-1b")
        result = gen(prompt, max_length=300, do_sample=True, temperature=0.7)[0]['generated_text']

        print("\n=== GENERATED REPORT ===\n")
        print(result)
        return result

    @task()
    def push_to_snowflake(report: str):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        # Ensure the anomaly reports table exists
        create_sql = """
            CREATE TABLE IF NOT EXISTS mars_anomaly_reports (
                id INT AUTOINCREMENT PRIMARY KEY,
                report TEXT,
                generated_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP
            )
        """
        hook.run(create_sql)

        insert_sql = """
            INSERT INTO mars_anomaly_reports (report)
            VALUES (%s)
        """
        hook.run(insert_sql, parameters=[report])
        print("[report] Anomaly report saved to Snowflake.")

    push_to_snowflake(generate_report(load_anomaly()))

dag = anomaly_report_generator()