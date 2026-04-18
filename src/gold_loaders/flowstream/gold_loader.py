# Databricks notebook source
import dlt
import yaml
from pyspark.sql import functions as F

# 1. Get catalog from Bundle config
catalog = spark.conf.get("bundle.catalog")

# 2. Load Gold metadata from config.yml
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

silver_schema = config["silver_schema"]   # silver_flowstream
gold_views    = config["gold_views"]

# ---------------------------------------------------------------------------
# 3. Dynamic Gold table builder
# ---------------------------------------------------------------------------
def create_gold_table(view_meta):
    name         = view_meta["name"]
    primary_src  = view_meta["primary_source"]
    join_src     = view_meta.get("join_source")
    join_key     = view_meta.get("join_key")
    description  = view_meta.get("description", "")
    lc_cfg       = view_meta.get("liquid_clustering", {})
    partition_by = view_meta.get("partition_by", None)

    @dlt.table(
        name=name,
        comment=description,
        table_properties={
            "quality": "gold",
            "delta.enableDeletionVectors": "true"
        },
        partition_cols=[partition_by] if partition_by else None,
        cluster_by=lc_cfg.get("cluster_by") if lc_cfg.get("enabled") else None
    )
    def build_gold():
        df = dlt.read(f"{silver_schema}.{primary_src}")

        if join_src and join_key:
            join_df = dlt.read(f"{silver_schema}.{join_src}")
            # Use explicit aliases to avoid ambiguous column references after join
            df = df.alias("primary").join(
                join_df.alias("secondary"),
                on=join_key,
                how="left"
            )

        if name == "daily_revenue_summary":
            return (
                df.groupBy(
                    F.col("primary.order_date"),
                    F.col("primary.country_code"),  # explicitly from orders_clean
                    F.col("primary.order_status")
                )
                .agg(
                    F.sum("primary.order_amount").alias("total_revenue"),
                    F.count("primary.order_id").alias("order_count"),
                    F.avg("primary.order_amount").alias("avg_order_value")
                )
                .withColumn("revenue_tier",
                    F.when(F.col("total_revenue") >= 10000, "High")
                     .when(F.col("total_revenue") >= 5000, "Medium")
                     .otherwise("Low")
                )
            )

        elif name == "order_status_pivot":
            return (
                df.groupBy(
                    F.col("order_date"),
                    F.col("order_status")
                )
                .agg(
                    F.count("order_id").alias("order_count"),
                    F.sum("order_amount").alias("total_value")
                )
                .withColumn("health_flag",
                    F.when(F.col("order_status") == "returned", "Watch")
                     .otherwise("OK")
                )
            )

        elif name == "product_performance":
            return (
                df.groupBy(
                    F.col("primary.product_id"),
                    F.col("secondary.product_name"),
                    F.col("secondary.category")
                )
                .agg(
                    F.sum("primary.order_amount").alias("total_revenue"),
                    F.count("primary.order_id").alias("units_sold"),
                    F.avg("primary.order_amount").alias("avg_revenue_per_order")
                )
                .withColumn("performance_band",
                    F.when(F.col("units_sold") >= 100, "Top Seller")
                     .when(F.col("units_sold") >= 50, "Mid Tier")
                     .otherwise("Low Volume")
                )
            )

        return df

# 4. Loop and register all Gold tables
for v in gold_views:
    create_gold_table(v)