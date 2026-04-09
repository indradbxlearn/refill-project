# Databricks notebook source
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="silver_hydropulse",
    comment="Cleaned HydroPulse telemetry with inferred schema"
)
def silver_hydropulse():
    # Read from the bronze table we just deployed
    df = dlt.readStream("bronze_hydropulse")
    
    # Standard cleaning: Add ingestion timestamp and handle potential nulls in keys
    return (
        df.withColumn("ingestion_timestamp", F.current_timestamp())
          .filter(F.col("pump_id").isNotNull())
          # We don't list 1,000 columns; they all pass through automatically!
    )