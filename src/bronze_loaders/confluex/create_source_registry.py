# Databricks notebook source
# Creates the source_registry control table — run once, idempotent

catalog_name = dbutils.widgets.get("catalog_name")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze_confluex")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.bronze_confluex.source_registry (
        source_id           STRING      NOT NULL,
        source_type         STRING      NOT NULL,
        entity              STRING      NOT NULL,
        format              STRING,
        base_path           STRING,
        lakeflow_table      STRING,
        api_endpoint        STRING,
        api_response_key    STRING,
        row_tag             STRING,
        sheet_name          STRING,
        delimiter           STRING,
        has_header          BOOLEAN,
        encoding            STRING,
        active              BOOLEAN     NOT NULL,
        registered_at       TIMESTAMP   NOT NULL,
        updated_at          TIMESTAMP   NOT NULL
    )
    USING DELTA
    COMMENT 'Central registry of all Confluex sources — Volume files, Lakeflow Connect, REST API'
""")

print(f"✓ source_registry created at {catalog_name}.bronze_confluex.source_registry")