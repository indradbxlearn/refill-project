# Databricks notebook source
# Databricks notebook source
# 1. SQL Setup for Infrastructure
# Using spark.sql to run the DDL commands
catalog = "bi_dev" # Adjust if needed for QA/Prod

print(f"Setting up infrastructure in {catalog}...")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.refill_arrivals")

# 2. Data Generation Logic
import pandas as pd
from datetime import datetime, date

base_path = f"/Volumes/{catalog}/landing/refill_arrivals/aquaflow"

data_payloads = {
    "flow_init_setup": {
        "setup_id": ["S-001", "S-002"],
        "technician_name": ["Indrajit", "Mitra"],
        "is_active": [True, False],
        "recorded_at": [datetime.now(), datetime.now()]
    },
    "pressure_kpi_tracker": {
        "customer_id": [101, 102],
        "kpi_score": [92.5, 88.0],
        "status": ["Stable", "Warning"],
        "event_date": [date(2026, 3, 28), date(2026, 3, 29)]
    },
    "dispenser_cleaning_logs": {
        "unit_id": ["D-99", "D-100"],
        "cleaner_name": ["AutoSystem", "Manual_Op"],
        "duration_mins": [15.5, 20.0]
    },
    "customer_service_center": {
        "ticket_id": ["T_55", "T_56"],
        "region": ["Kolkata", "London"],
        "priority": ["High", "Medium"]
    },
    "eservice_logs": {
        "log_id": ["LOG_001", "LOG_002"],
        "error_code": [0, 404],
        "message": ["System Healthy", "Page Not Found"]
    },
    "normal_delivery_schedule": {
        "route_id": ["R_12", "R_15"],
        "driver": ["Raj", "Sam"],
        "eta": ["14:00", "16:30"]
    }
}

# 3. Execution Loop
for table_name, content in data_payloads.items():
    print(f"Generating physical files for: {table_name}")
    folder_path = f"{base_path}/{table_name}"
    dbutils.fs.mkdirs(folder_path)
    
    df = spark.createDataFrame(pd.DataFrame(content))
    df.write.mode("overwrite").parquet(folder_path)

print(f"SUCCESS: Infrastructure and Sample Data ready in {base_path}")
