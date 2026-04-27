# Databricks notebook source
import json
from datetime import datetime

catalog_name   = dbutils.widgets.get("catalog_name")
source_system  = "confluex"
registry_table = f"{catalog_name}.bronze_confluex.source_registry"
scope          = "confluex-scope"

# ── HELPER: upsert into registry ─────────────────────────────────────────────
def register_source(source_id, source_type, entity,
                    format=None, base_path=None,
                    lakeflow_table=None, api_endpoint=None,
                    api_response_key=None, row_tag=None,
                    sheet_name=None, delimiter=None,
                    has_header=None, encoding="UTF-8"):
    spark.sql(f"""
        MERGE INTO {registry_table} AS target
        USING (
            SELECT
                '{source_id}'                                               AS source_id,
                '{source_type}'                                             AS source_type,
                '{entity}'                                                  AS entity,
                {f"'{format}'"          if format           else "NULL"}    AS format,
                {f"'{base_path}'"       if base_path        else "NULL"}    AS base_path,
                {f"'{lakeflow_table}'"  if lakeflow_table   else "NULL"}    AS lakeflow_table,
                {f"'{api_endpoint}'"    if api_endpoint     else "NULL"}    AS api_endpoint,
                {f"'{api_response_key}'" if api_response_key else "NULL"}   AS api_response_key,
                {f"'{row_tag}'"         if row_tag          else "NULL"}    AS row_tag,
                {f"'{sheet_name}'"      if sheet_name       else "NULL"}    AS sheet_name,
                {f"'{delimiter}'"       if delimiter        else "NULL"}    AS delimiter,
                {str(has_header).upper() if has_header is not None else "NULL"}
                                                                            AS has_header,
                '{encoding}'                                                AS encoding,
                true                                                        AS active,
                CURRENT_TIMESTAMP                                           AS registered_at,
                CURRENT_TIMESTAMP                                           AS updated_at
        ) AS source
        ON  target.source_id = source.source_id
        AND target.entity    = source.entity
        WHEN MATCHED THEN UPDATE SET
            target.format           = source.format,
            target.base_path        = source.base_path,
            target.lakeflow_table   = source.lakeflow_table,
            target.api_endpoint     = source.api_endpoint,
            target.api_response_key = source.api_response_key,
            target.row_tag          = source.row_tag,
            target.sheet_name       = source.sheet_name,
            target.delimiter        = source.delimiter,
            target.has_header       = source.has_header,
            target.encoding         = source.encoding,
            target.active           = source.active,
            target.updated_at       = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"  ✓ Registered [{source_type}] {source_id} → {entity}")

# ── 1. VOLUME SOURCES ─────────────────────────────────────────────────────────
print("\n── Scanning Volume sources...")
base_volume = f"/Volumes/{catalog_name}/landing/refill_arrivals/{source_system}/"

try:
    customer_folders = [
        item for item in dbutils.fs.ls(base_volume)
        if item.isDir()
        and not item.name.startswith("_")
        and not item.name.startswith(".")
    ]

    entities = ["products_raw", "users_raw", "carts_raw"]

    for folder in customer_folders:
        customer_id   = folder.name.rstrip("/")
        metadata_path = f"{folder.path}_metadata.json"

        # Read _metadata.json
        try:
            raw  = dbutils.fs.head(metadata_path)
            meta = json.loads(raw)
        except:
            meta = {"format": "json"}  # safe default

        fmt        = meta.get("format",     "json")
        row_tag    = meta.get("row_tag",    None)
        sheet_name = meta.get("sheet_name", None)
        delimiter  = meta.get("delimiter",  None)
        has_header = meta.get("has_header", None)
        encoding   = meta.get("encoding",   "UTF-8")

        for entity in entities:
            entity_path = f"{folder.path}{entity}/"
            try:
                dbutils.fs.ls(entity_path)
                register_source(
                    source_id  = f"volume_{customer_id}",
                    source_type= "volume",
                    entity     = entity,
                    format     = fmt,
                    base_path  = entity_path,
                    row_tag    = row_tag,
                    sheet_name = sheet_name,
                    delimiter  = delimiter,
                    has_header = has_header,
                    encoding   = encoding
                )
            except:
                pass

except Exception as e:
    print(f"  ⚠ Volume scan skipped: {e}")

# ── 2. LAKEFLOW CONNECT SOURCES ───────────────────────────────────────────────
print("\n── Registering Lakeflow Connect sources...")

lakeflow_sources = [
    ("products_raw", f"{catalog_name}.lakeflow_confluex.products"),
    ("users_raw",    f"{catalog_name}.lakeflow_confluex.users"),
    ("carts_raw",    f"{catalog_name}.lakeflow_confluex.carts"),
]

for entity, lakeflow_table in lakeflow_sources:
    register_source(
        source_id      = "lakeflow_s3",
        source_type    = "lakeflow_connect",
        entity         = entity,
        lakeflow_table = lakeflow_table
    )

# ── 3. REST API SOURCES ───────────────────────────────────────────────────────
print("\n── Registering REST API sources...")

api_sources = [
    ("products_raw", "/auth/products", "products"),
    ("users_raw",    "/auth/users",    "users"),
    ("carts_raw",    "/auth/carts",    "carts"),
]

for entity, endpoint, response_key in api_sources:
    register_source(
        source_id        = "dummyjson_api",
        source_type      = "rest_api",
        entity           = entity,
        api_endpoint     = endpoint,
        api_response_key = response_key
    )

# ── Show final registry ───────────────────────────────────────────────────────
print("\n── Current source_registry:")
display(spark.table(registry_table).orderBy("entity", "source_type"))