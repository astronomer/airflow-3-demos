from airflow.sdk import Asset, dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
from sklearn.ensemble import IsolationForest
import numpy as np
import json

# Declare the output asset for downstream triggering
confirmed_anomaly_asset = Asset("confirmed_anomaly")

@dag(start_date=datetime(2024, 12, 1), schedule=[Asset("preprocess_probe_data")], tags=['earth'])
def earth_analysis():

    @task()
    def receive_data():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        sql = """
            SELECT temperature, radiation, version
            FROM mars_probe_telemetry
            WHERE temperature IS NOT NULL AND radiation IS NOT NULL
            ORDER BY collected_at DESC
            LIMIT 1
        """
        record = hook.get_first(sql)
        if not record:
            raise Exception("[earth] No telemetry data found in Snowflake.")
        
        temp, radiation, version = record
        data = {
            "temp": temp,
            "radiation": radiation,
            "version": version
        }
        print(f"[earth] Received data from Snowflake: {data}")
        return json.dumps(data)

    @task(outlets=[confirmed_anomaly_asset])
    def analyze_data(data):
        print("[earth] Starting analysis of probe data...")

        try:
            parsed = json.loads(data)
            temp = parsed.get("temp")
            radiation = parsed.get("radiation", "unknown")
            version = parsed.get("version", "unknown")
            print(f"[earth] Parsed - Temp: {temp}, Radiation: {radiation}, Version: {version}")
        except json.JSONDecodeError:
            print("[earth] Error decoding data")
            return "error"

        if temp is None:
            print("[earth] Missing temperature")
            return "error"

        # Encode radiation levels
        radiation_map = {"low": 0, "medium": 1, "high": 2}
        radiation_val = radiation_map.get(radiation.lower(), 1)

        # Prepare input for ML model
        X = np.array([[temp, radiation_val]])

        # Simulated model training with historical samples
        dummy_X = np.array([
            [-65, 1],
            [-60, 0],
            [-68, 1],
            [-62, 1],
            [-80, 2],  # outlier
        ])
        model = IsolationForest(contamination=0.1, random_state=42)
        model.fit(dummy_X)

        prediction = model.predict(X)[0]  # -1 = anomaly
        is_anomaly = prediction == -1
        print(f"[earth] ML model anomaly prediction: {'YES' if is_anomaly else 'NO'}")

        score = round(1 - model.decision_function(X)[0], 2)
        print(f"[earth] ML anomaly score: {score}")

        status = "CRITICAL" if is_anomaly or score > 0.7 else "NORMAL"
        print(f"[earth] Final status: {status}")

        return {
            "status": status,
            "score": score,
            "source_version": version
        }

    analyze_data(receive_data())

earth_analysis()