from airflow.sdk import Asset, dag, task
from datetime import datetime, timedelta
import json
import random

probe_asset = Asset("collect_probe_data")

@dag(start_date=datetime(2024, 12, 1), schedule=[Asset("preprocess_probe_data")], tags=['earth'])
def earth_analysis():

    @task()
    def receive_data():
        with open(probe_asset.uri, 'r') as f:
            content = f.read()
        print(f"[earth] Received data: {content}")
        return content

    @task()
    def analyze_data(data):
        print("[earth] Starting analysis of probe data...")
        try:
            parsed = json.loads(data)
            temp = parsed.get("temp", "N/A")
            radiation = parsed.get("radiation", "unknown")
            version = parsed.get("version", "unknown")
            print(f"[earth] Parsed - Temp: {temp}, Radiation: {radiation}, Version: {version}")
        except json.JSONDecodeError:
            print("[earth] Error decoding data")
            return "error"

        anomaly = temp < -70 or radiation == "high"
        print(f"[earth] Anomaly detected: {'YES' if anomaly else 'NO'}")

        risk_score = random.uniform(0.3, 0.8)
        if anomaly:
            risk_score += 0.2
        risk_score = round(min(risk_score, 1.0), 2)
        print(f"[earth] Risk score: {risk_score}")

        status = "CRITICAL" if risk_score > 0.85 else "NORMAL"
        print(f"[earth] Final status: {status}")

        return {
            "status": status,
            "score": risk_score,
            "source_version": version
        }

    analyze_data(receive_data())

earth_analysis()