# Databricks notebook source
# This notebook is triggered as the final task in the Workflow
# Runs OPTIMIZE + VACUUM on Silver and Gold tables after pipeline completion

catalog_name = dbutils.widgets.get("catalog_name")

tables_to_optimize = [
    f"{catalog_name}.silver_flowstream.orders_clean",
    f"{catalog_name}.silver_flowstream.products_clean",
    f"{catalog_name}.silver_flowstream.customers_clean"
]

for table in tables_to_optimize:
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {table}")
    print(f"Vacuuming {table}...")
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")  # 7 days retention

print("All tables optimized and vacuumed successfully.")