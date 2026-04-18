# Databricks notebook source
import dlt
import yaml

# 1. Get catalog from Bundle config
catalog = spark.conf.get("bundle.catalog")

# 2. Load metadata from config.yml
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

source_system = config["source_system"]
tables = config["tables"]
file_format = config["reader_options"]["format"]

# 3. Dynamic Bronze Streaming Table creation using Auto Loader (cloudFiles)
def create_bronze_streaming_table(table_meta):
    t_name = table_meta["name"]

    @dlt.table(
        name=t_name,
        comment=f"Bronze streaming table for {t_name} — raw {file_format} from S3 via Auto Loader",
        table_properties={
            "quality": "bronze",
            "pipelines.autoOptimize.managed": "true"
        }
    )
    @dlt.expect("non_null_input_file", "_metadata.file_path IS NOT NULL")
    def load_from_volume():
        path = f"/Volumes/{catalog}/landing/refill_arrivals/{source_system}/{t_name}/"

        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(path)
        )

# 4. Loop and register all Bronze streaming tables
for t in tables:
    create_bronze_streaming_table(t)