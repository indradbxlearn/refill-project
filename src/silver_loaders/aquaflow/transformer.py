import dlt
import yaml
from pyspark.sql import functions as F

# 1. Load the Silver metadata
with open("config.yml", 'r') as f:
    config = yaml.safe_load(f)

tables = config['tables']

# 2. Define the Dynamic Transformation Logic
def create_silver_table(table_meta):
    target_name = table_meta['name']
    source_name = table_meta['source_table']
    column_map = table_meta['columns'] # Dictionary of col:type
    
    @dlt.table(
        name=target_name,
        comment=f"Cleaned Silver table for {target_name}"
    )
    def transform_data():
        # Read from the LIVE bronze table (DLT dependency)
        df = dlt.read(source_name)
        
        # Dynamically apply casting based on your YAML config
        for col_name, col_type in column_map.items():
            df = df.withColumn(col_name, F.col(col_name).cast(col_type))
            
        return df

# 3. Register all Silver tables
for t in tables:
    create_silver_table(t)