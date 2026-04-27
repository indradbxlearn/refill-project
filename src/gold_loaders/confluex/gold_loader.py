# Databricks notebook source
import dlt
import yaml
from pyspark.sql import functions as F

# 1. Get catalog from Bundle config
catalog = spark.conf.get("bundle.catalog")

# 2. Load Gold metadata from config.yml
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

silver_schema = config["silver_schema"]   # silver_confluex
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

    @dlt.table(
        name=name,
        comment=description,
        table_properties={
            "quality":                        "gold",
            "delta.enableDeletionVectors":    "true"
        },
        cluster_by=lc_cfg.get("cluster_by") if lc_cfg.get("enabled") else None
    )
    def build_gold():
        df = dlt.read(f"{silver_schema}.{primary_src}")

        if join_src and join_key:
            join_df = dlt.read(f"{silver_schema}.{join_src}")
            df = df.alias("primary").join(
                join_df.alias("secondary"),
                on=join_key,
                how="left"
            )

        # ── source_contribution_summary ───────────────────────────────────────
        # The STAR table of Confluex — proves all 3 sources are flowing in
        if name == "source_contribution_summary":
            return (
                df.groupBy(
                    F.col("_source"),
                    F.col("cart_status")
                )
                .agg(
                    F.count("cart_id").alias("total_carts"),
                    F.sum("total_amount").alias("total_revenue"),
                    F.avg("total_amount").alias("avg_cart_value"),
                    F.countDistinct("user_id").alias("unique_users")
                )
                .withColumn("revenue_contribution_pct",
                    F.round(
                        F.col("total_revenue") /
                        F.sum("total_revenue").over(
                            __import__("pyspark.sql.window", fromlist=["Window"])
                            .Window.partitionBy(F.lit(1))
                        ) * 100,
                        2
                    )
                )
            )

        # ── product_cart_summary ──────────────────────────────────────────────
        elif name == "product_cart_summary":
            return (
                df.groupBy(
                    F.col("primary.product_id"),
                    F.col("secondary.product_name"),
                    F.col("secondary.category"),
                    F.col("primary._source")
                )
                .agg(
                    F.sum("primary.total_amount").alias("total_revenue"),
                    F.sum("primary.quantity").alias("total_units_sold"),
                    F.count("primary.cart_id").alias("total_orders"),
                    F.avg("primary.total_amount").alias("avg_order_value")
                )
                .withColumn("performance_band",
                    F.when(F.col("total_units_sold") >= 50, "Top Seller")
                     .when(F.col("total_units_sold") >= 20, "Mid Tier")
                     .otherwise("Low Volume")
                )
            )

        # ── user_purchase_summary ─────────────────────────────────────────────
        elif name == "user_purchase_summary":
            return (
                df.groupBy(
                    F.col("primary.user_id"),
                    F.col("secondary.first_name"),
                    F.col("secondary.last_name"),
                    F.col("secondary.country_code"),
                    F.col("secondary.segment"),
                    F.col("primary._source")
                )
                .agg(
                    F.sum("primary.total_amount").alias("total_spend"),
                    F.count("primary.cart_id").alias("total_carts"),
                    F.avg("primary.total_amount").alias("avg_cart_value"),
                    F.max("primary.updated_at").alias("last_activity")
                )
                .withColumn("customer_tier",
                    F.when(F.col("total_spend") >= 5000, "Platinum")
                     .when(F.col("total_spend") >= 2000, "Gold")
                     .when(F.col("total_spend") >= 500,  "Silver")
                     .otherwise("Bronze")
                )
            )

        return df

# 4. Loop and register all Gold tables
for v in gold_views:
    create_gold_table(v)