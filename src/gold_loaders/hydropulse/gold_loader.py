# Databricks notebook source
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="gold_pump_efficiency",
    comment="Hourly summary of pump performance"
)
def gold_pump_efficiency():
    return (
        dlt.read("silver_hydropulse")
          # Watermark handles late data (e.g., wait up to 10 mins)
          .withWatermark("timestamp", "10 minutes") 
          .groupBy("pump_id", F.window("timestamp", "1 hour"))
          .agg(
              # Using sensor_001 and sensor_002 from our generator
              F.avg("sensor_001").alias("avg_sensor_01_val"),
              F.max("sensor_002").alias("max_sensor_02_val")
          )
    )