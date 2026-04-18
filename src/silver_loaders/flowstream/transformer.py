# Databricks notebook source
import dlt
import yaml
from pyspark.sql import functions as F

# 1. Get catalog from Bundle config
catalog = spark.conf.get("bundle.catalog")

# 2. Load Silver metadata from config.yml
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

tables = config["tables"]
source_schema = config["source_schema"]   # bronze_flowstream

# ---------------------------------------------------------------------------
# 3a. CDC tables — use APPLY CHANGES INTO (SCD Type 1)
# ---------------------------------------------------------------------------
def create_silver_cdc_table(table_meta):
    target_name  = table_meta["name"]
    source_name  = table_meta["source_table"]
    cdc_cfg      = table_meta["cdc"]
    column_map   = table_meta["columns"]
    expectations = table_meta.get("expectations", [])
    lc_cfg       = table_meta.get("liquid_clustering", {})
    partition_by = table_meta.get("partition_by", None)

    table_props = {"quality": "silver"}
    if lc_cfg.get("enabled"):
        table_props["delta.enableDeletionVectors"] = "true"

    drop_expectations = {
        exp["name"]: exp["constraint"]
        for exp in expectations if exp["action"] == "drop"
    }
    warn_expectations = {
        exp["name"]: exp["constraint"]
        for exp in expectations if exp["action"] == "warn"
    }

    dlt.create_streaming_table(
        name=target_name,
        comment=f"Silver CDC streaming table for {target_name}",
        table_properties=table_props,
        partition_cols=[partition_by] if partition_by else None,
        cluster_by=lc_cfg.get("cluster_by") if lc_cfg.get("enabled") else None,
        expect_all_or_drop=drop_expectations if drop_expectations else None,
        expect_all=warn_expectations if warn_expectations else None
    )

    dlt.apply_changes(
        target=target_name,
        source=f"{source_schema}.{source_name}",
        keys=cdc_cfg["keys"],
        sequence_by=F.col(cdc_cfg["sequence_by"]),
        stored_as_scd_type=cdc_cfg["stored_as_scd_type"],
        apply_as_truncates=None,
        apply_as_deletes=None,
        column_list=[c for c in column_map.keys()]
    )

# ---------------------------------------------------------------------------
# 3b. Non-CDC tables — standard DLT Streaming Table with type casting
# ---------------------------------------------------------------------------
def create_silver_streaming_table(table_meta):
    target_name  = table_meta["name"]
    source_name  = table_meta["source_table"]
    column_map   = table_meta["columns"]
    expectations = table_meta.get("expectations", [])
    lc_cfg       = table_meta.get("liquid_clustering", {})

    drop_expectations = {
        exp["name"]: exp["constraint"]
        for exp in expectations if exp["action"] == "drop"
    }

    @dlt.table(
        name=target_name,
        comment=f"Silver streaming table for {target_name} — cleansed and typed",
        table_properties={"quality": "silver"},
        cluster_by=lc_cfg.get("cluster_by") if lc_cfg.get("enabled") else None
    )
    @dlt.expect_all_or_drop(drop_expectations)
    def transform_streaming():
        df = dlt.read_stream(f"{source_schema}.{source_name}")
        for col_name, col_type in column_map.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(col_type))
        return df

# ---------------------------------------------------------------------------
# 4. Loop and register — route to CDC or streaming based on config
# ---------------------------------------------------------------------------
for t in tables:
    if t["cdc"]["enabled"]:
        create_silver_cdc_table(t)
    else:
        create_silver_streaming_table(t)