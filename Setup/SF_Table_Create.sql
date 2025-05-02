CREATE TABLE mars_probe_telemetry (
    id BIGINT AUTOINCREMENT,
    collected_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    temperature INT,
    radiation STRING,
    version STRING
);