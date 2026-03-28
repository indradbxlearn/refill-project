import dlt
import yaml

# 1. Get the catalog from the Bundle config (passed via resources YAML)
catalog = spark.conf.get("bundle.catalog")

# 2. Load the metadata from the local config.yml
with open("config.yml", 'r') as f:
    config = yaml.safe_load(f)

source_system = config['source_system']
tables = config['tables']

# 3. Define the Dynamic Loading Logic
def create_bronze_table(table_meta):
    t_name = table_meta['name']
    
    @dlt.table(name=t_name, comment=f"Raw data for {t_name}")
    def load_from_volume():
        # Points to your landing volume based on the catalog (bi_dev/bi_qa)
        path = f"/Volumes/{catalog}/landing/refill_arrivals/{source_system}/{t_name}/"
        
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(path)
        )

# 4. Loop and Register
for t in tables:
    create_bronze_table(t)