# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# Pull the values we just defined in the YAML configuration block
env = spark.conf.get("bundle.target")
catalog = spark.conf.get("bundle.catalog")

# Construct the path dynamically
# This will result in: /Volumes/bi_dev/landing/refill_arrivals/hydropulse/telemetry_stream
landing_path = f"/Volumes/{catalog}/landing/refill_arrivals/hydropulse/telemetry_stream"

@dlt.table(name="bronze_hydropulse")
def bronze_hydropulse():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(landing_path)
    )