# Databricks notebook source

# MAGIC %pip install openpyxl lxml

# COMMAND ----------

import dlt
import yaml
import json
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from functools import reduce

# ── 1. Get config from Bundle ─────────────────────────────────────────────────
catalog = spark.conf.get("bundle.catalog")
scope   = spark.conf.get("bundle.scope")

# ── 2. Load config.yml ────────────────────────────────────────────────────────
try:
    notebook_path = dbutils.notebook.entry_point \
                        .getDbutils().notebook().getContext() \
                        .notebookPath().get()
    notebook_dir  = "/".join(notebook_path.split("/")[:-1])
    config_path   = f"/Workspace{notebook_dir}/config.yml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
except Exception:
    config = {
        "schema": "bronze_confluex",
        "source_system": "confluex",
        "entities": [
            {"name": "products_raw", "primary_key": "product_id"},
            {"name": "users_raw",    "primary_key": "user_id"},
            {"name": "carts_raw",    "primary_key": "cart_id"},
        ]
    }

entities       = config["entities"]
registry_table = f"{catalog}.bronze_confluex.source_registry"

# COMMAND ----------

# ── 3. Read active sources from registry ─────────────────────────────────────
registry = (
    spark.read
    .table(registry_table)
    .filter(F.col("active") == True)
    .collect()
)

# COMMAND ----------

# ── 4. Get JWT token from dummyjson ──────────────────────────────────────────
def get_api_token():
    username = dbutils.secrets.get(scope=scope, key="dummyjson-username")
    password = dbutils.secrets.get(scope=scope, key="dummyjson-password")
    response = requests.post(
        "https://dummyjson.com/auth/login",
        json={"username": username, "password": password, "expiresInMins": 60}
    )
    return response.json().get("accessToken")

# ── 5. Fetch all records from REST API ───────────────────────────────────────
def fetch_api_data(endpoint, response_key, token):
    headers  = {"Authorization": f"Bearer {token}"}
    url      = f"https://dummyjson.com{endpoint}?limit=0"
    response = requests.get(url, headers=headers)
    records  = response.json().get(response_key, [])
    flat = []
    for r in records:
        flat_r = {}
        for k, v in r.items():
            if isinstance(v, (dict, list)):
                flat_r[k] = json.dumps(v)
            else:
                flat_r[k] = str(v) if v is not None else None
        flat_r["_source"] = "rest_api_dummyjson"
        flat.append(flat_r)
    return flat

# COMMAND ----------

# ── 6a. Python-based XML reader — handles directory path ─────────────────────
def read_xml_as_spark(dbfs_path: str, row_tag: str, source_id: str, encoding: str = "UTF-8"):
    # List all XML files in the directory
    try:
        files = [
            f.path for f in dbutils.fs.ls(dbfs_path)
            if f.name.endswith(".xml")
        ]
    except:
        files = [dbfs_path]  # fallback — treat as single file path

    all_rows = []
    for fpath in files:
        # Convert dbfs: path to local path for Python's file IO
        local_path = fpath.replace("dbfs:", "")
        try:
            tree = ET.parse(local_path)
            root = tree.getroot()
            for element in root.iter(row_tag):
                row = {child.tag: child.text for child in element}
                row.update(element.attrib)
                all_rows.append(row)
        except Exception as e:
            print(f"  ⚠ Could not parse XML {fpath}: {e}")

    if not all_rows:
        return spark.createDataFrame([], StructType([]))

    pdf = pd.DataFrame(all_rows)
    return (
        spark.createDataFrame(pdf)
        .withColumn("_source", F.lit(source_id))
    )

# ── 6b. Python-based Excel reader — handles directory path ───────────────────
def read_excel_as_spark(dbfs_path: str, sheet_name, source_id: str, has_header: str = "true"):
    # List all XLSX files in the directory
    try:
        files = [
            f.path for f in dbutils.fs.ls(dbfs_path)
            if f.name.endswith(".xlsx")
        ]
    except:
        files = [dbfs_path]  # fallback — treat as single file path

    header_arg     = 0 if str(has_header).lower() == "true" else None
    resolved_sheet = sheet_name if sheet_name else 0
    all_dfs        = []

    for fpath in files:
        local_path = fpath.replace("dbfs:", "")
        try:
            pdf = pd.read_excel(
                local_path,
                sheet_name=resolved_sheet,
                header=header_arg,
                engine="openpyxl"
            )
            all_dfs.append(pdf)
        except Exception as e:
            print(f"  ⚠ Could not read XLSX {fpath}: {e}")

    if not all_dfs:
        return spark.createDataFrame([], StructType([]))

    combined = pd.concat(all_dfs, ignore_index=True)
    return (
        spark.createDataFrame(combined.astype(str))
        .withColumn("_source", F.lit(source_id))
    )

# ── 6c. Route to correct reader based on format ───────────────────────────────
def get_volume_reader(src, path):
    fmt        = src.format or "json"
    row_tag    = src.row_tag
    sheet_name = src.sheet_name or 0
    delimiter  = src.delimiter or ","
    has_header = str(src.has_header).lower() if src.has_header is not None else "true"
    encoding   = src.encoding or "UTF-8"
    source_id  = src.source_id

    # Normalise paths for both readers
    local_path = path.replace("dbfs:", "")
    cloud_path = f"dbfs:{path}" if not path.startswith("dbfs:") else path

    if fmt == "xml":
        return read_xml_as_spark(
            dbfs_path = local_path,
            row_tag   = row_tag or "record",
            source_id = source_id,
            encoding  = encoding
        )

    elif fmt == "xlsx":
        return read_excel_as_spark(
            dbfs_path  = local_path,
            sheet_name = sheet_name,
            source_id  = source_id,
            has_header = has_header
        )

    else:
        # json, csv, parquet, avro, orc — via cloudFiles Auto Loader
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",             fmt)
            .option("cloudFiles.schemaEvolutionMode","addNewColumns")
            .option("cloudFiles.inferColumnTypes",   "true")
            .option("header",                        has_header)
            .option("delimiter",                     delimiter)
            .option("encoding",                      encoding)
            .load(cloud_path)
            .withColumn("_source", F.lit(source_id))
        )

# COMMAND ----------

# ── 7. Build per-entity source tables then unify ─────────────────────────────
def create_bronze_tables(entity_meta):
    entity_name    = entity_meta["name"]
    table_names    = []
    entity_sources = [r for r in registry if r.entity == entity_name]

    for src in entity_sources:
        source_type = src.source_type
        source_id   = src.source_id
        table_name  = f"{entity_name}_{source_type}_{source_id}".replace("-", "_")

        # ── VOLUME source ─────────────────────────────────────────────────────
        if source_type == "volume":
            fmt       = src.format or "json"
            base_path = src.base_path

            if fmt == "xlsx":
                @dlt.table(
                    name=table_name,
                    comment=f"Bronze — {entity_name} from Volume XLSX ({source_id})",
                    table_properties={"quality": "bronze", "source_type": "volume_xlsx"}
                )
                def load_xlsx(s=src, path=base_path):
                    return get_volume_reader(s, path)

            elif fmt == "xml":
                @dlt.table(
                    name=table_name,
                    comment=f"Bronze — {entity_name} from Volume XML ({source_id})",
                    table_properties={"quality": "bronze", "source_type": "volume_xml"}
                )
                def load_xml(s=src, path=base_path):
                    return get_volume_reader(s, path)

            else:
                # json, csv, parquet, avro, orc — streaming via cloudFiles
                @dlt.table(
                    name=table_name,
                    comment=f"Bronze — {entity_name} from Volume {fmt} ({source_id})",
                    table_properties={"quality": "bronze", "source_type": "volume"}
                )
                def load_volume(s=src, path=base_path):
                    return get_volume_reader(s, path)

            table_names.append(table_name)

        # ── LAKEFLOW CONNECT source ───────────────────────────────────────────
        elif source_type == "lakeflow_connect":
            lakeflow_table = src.lakeflow_table

            @dlt.table(
                name=table_name,
                comment=f"Bronze — {entity_name} from Lakeflow Connect (S3)",
                table_properties={"quality": "bronze", "source_type": "lakeflow_connect"}
            )
            def load_lakeflow(lf_table=lakeflow_table):
                return (
                    dlt.read_stream(lf_table)
                    .withColumn("_source", F.lit("lakeflow_s3"))
                )

            table_names.append(table_name)

        # ── REST API source ───────────────────────────────────────────────────
        elif source_type == "rest_api":
            api_endpoint     = src.api_endpoint
            api_response_key = src.api_response_key

            @dlt.table(
                name=table_name,
                comment=f"Bronze — {entity_name} from REST API (dummyjson)",
                table_properties={"quality": "bronze", "source_type": "rest_api"}
            )
            def load_api(endpoint=api_endpoint, response_key=api_response_key):
                token   = get_api_token()
                records = fetch_api_data(endpoint, response_key, token)
                return spark.createDataFrame(records)

            table_names.append(table_name)

    # ── UNIFIED Bronze table — union all sources ──────────────────────────────
    if table_names:
        @dlt.table(
            name=entity_name,
            comment=f"Unified Bronze — {entity_name} from all sources",
            table_properties={
                "quality":                        "bronze",
                "pipelines.autoOptimize.managed": "true"
            }
        )
        def unify(tnames=table_names):
            dfs = []
            for tname in tnames:
                try:
                    dfs.append(dlt.read_stream(tname))
                except:
                    try:
                        dfs.append(dlt.read(tname))
                    except:
                        pass
            if not dfs:
                return spark.createDataFrame([], StructType([]))
            return reduce(
                lambda a, b: a.unionByName(b, allowMissingColumns=True),
                dfs
            )

# COMMAND ----------

# ── 8. Loop and register all entities ────────────────────────────────────────
for entity in entities:
    create_bronze_tables(entity)