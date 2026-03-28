# Databricks notebook source
import dlt
import yaml
from pyspark.sql import functions as F

# 1. Load the Silver metadata from the local config.yml
# This file should be in the same folder: src/silver_loaders/aquaflow/
with open("config.yml", 'r') as f:
    config = yaml.safe_load(f)

tables = config['tables']

# 2. Define the Dynamic Transformation Logic
def create_silver_table(table_meta):
    target_name = table_meta['name']
    source_name = table_meta['source_table']
    column_map = table_meta['columns'] # Dictionary of {col_name: col_type}
    
    @dlt.table(
        name=target_name,
        comment=f"Cleaned Silver table for {target_name} from Aquaflow"
    )
    def transform_data():
        # FIX: We use the schema prefix because the source 
        # table lives in a different DLT pipeline (Bronze).
        source_path = f"bronze_aquaflow.{source_name}"
        
        df = dlt.read(source_path)
        
        # Dynamically apply casting based on your YAML config
        # This handles the standardization for your 63 worksheets
        for col_name, col_type in column_map.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(col_type))
            
        return df

# 3. Loop through the config and register all Silver tables
for t in tables:
    create_silver_table(t)