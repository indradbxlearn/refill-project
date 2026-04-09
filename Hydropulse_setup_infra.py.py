# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Databricks notebook source
# 1. SQL Setup for Infrastructure
catalog = "bi_dev" 

print(f"Ensuring infrastructure exists in {catalog}...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.refill_arrivals")

# 2. Data Generation Logic for HydroPulse (High Column Volume)
import pandas as pd
import numpy as np
from datetime import datetime

base_path = f"/Volumes/{catalog}/landing/refill_arrivals/hydropulse"

# Creating 1000 sensor columns + 3 metadata columns
num_sensors = 1000
num_rows = 5

print(f"Preparing payload with {num_sensors} sensor columns...")

payload = {
    "pump_id": [f"PUMP_{i:03d}" for i in range(1, num_rows + 1)],
    "timestamp": [datetime.now()] * num_rows,
    "operational_status": ["Running", "Idle", "Running", "Warning", "Running"]
}

# Dynamically add sensor_001 to sensor_1000
for i in range(1, num_sensors + 1):
    payload[f"sensor_{i:03d}"] = np.random.uniform(20.0, 80.0, num_rows)

# 3. Execution
print(f"Generating physical files for: telemetry_stream")
folder_path = f"{base_path}/telemetry_stream"
dbutils.fs.mkdirs(folder_path)

# Convert to Spark and Write
df = spark.createDataFrame(pd.DataFrame(payload))
df.write.mode("overwrite").parquet(folder_path)

print(f"SUCCESS: Infrastructure and 1000-column sample data ready in {folder_path}")
