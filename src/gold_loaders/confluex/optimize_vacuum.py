# Databricks notebook source
# OPTIMIZE + VACUUM Silver tables only
# Gold tables are DLT managed — DLT handles optimization internally

catalog_name = dbutils.widgets.get("catalog_name")

tables_to_optimize = [
    f"{catalog_name}.silver_confluex.products_clean",
    f"{catalog_name}.silver_confluex.users_clean",
    f"{catalog_name}.silver_confluex.carts_clean",
]

for table in tables_to_optimize:
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {table}")
    print(f"Vacuuming {table}...")
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")

print("\n✓ All Silver tables optimized and vacuumed successfully.")