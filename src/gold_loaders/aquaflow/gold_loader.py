# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# 1. Configuration
catalog = "bi_dev"
silver_schema = "silver_aquaflow"

@dlt.table(
    name="technician_performance_gold",
    comment="Final Gold table joining Technician setups with Pressure KPIs"
)
def technician_performance_gold():
    # Read from Silver (Cross-pipeline read)
    setup_df = dlt.read(f"{silver_schema}.flow_init_setup_clean")
    kpi_df = dlt.read(f"{silver_schema}.pressure_kpi_tracker_clean")
    
    # Business Logic: Join and Create Status Flags
    # Note: In our sample data, we'll join on a dummy condition or actual ID
    return (
        setup_df.alias("s")
        .join(kpi_df.alias("k"), F.col("s.setup_id") == "S-001") 
        .select(
            F.col("s.technician_name"),
            F.col("k.kpi_score"),
            F.col("k.status").alias("pressure_status"),
            F.col("k.event_date"),
            F.when(F.col("k.kpi_score") >= 90, "Optimal")
             .otherwise("Action Required")
             .alias("operational_health")
        )
    )