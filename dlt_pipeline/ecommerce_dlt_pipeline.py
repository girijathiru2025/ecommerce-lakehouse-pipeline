# Databricks notebook source
# This file is the Delta Live Tables (DLT) pipeline definition.
# Create a new DLT pipeline in Databricks UI pointing to this notebook.
#
# DLT Pipeline settings (fill in UI):
#   Name         : ecommerce-lakehouse-dlt
#   Target schema: ecommerce_lakehouse.dlt_silver   (DLT manages its own schema)
#   Cluster mode : Enhanced autoscaling
#   Channel      : Current

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_PATH = spark.conf.get("raw_path", "dbfs:/FileStore/ecommerce/raw")

# ─────────────────────────────────────────────────────────────────────────────
# BRONZE — streaming ingest via Auto Loader
# ─────────────────────────────────────────────────────────────────────────────

def _bronze_stream(entity: str, schema: str):
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.schemaLocation", f"{RAW_PATH}/_schemas/{entity}")
             .schema(schema)
             .option("header", "true")
             .load(f"{RAW_PATH}/{entity}.csv")
             .withColumn("_ingest_ts",   F.current_timestamp())
             .withColumn("_source_file", F.input_file_name())
    )


@dlt.table(
    name="bronze_customers",
    comment="Raw customer records ingested from CSV via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return _bronze_stream(
        "customers",
        "customer_id STRING, first_name STRING, last_name STRING, email STRING, "
        "city STRING, state STRING, country STRING, signup_date TIMESTAMP, is_premium BOOLEAN",
    )


@dlt.table(
    name="bronze_products",
    comment="Raw product catalog ingested from CSV via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return _bronze_stream(
        "products",
        "product_id STRING, product_name STRING, category STRING, "
        "unit_price DOUBLE, cost_price DOUBLE, stock_qty INT, is_active BOOLEAN",
    )


@dlt.table(
    name="bronze_orders",
    comment="Raw order headers ingested from CSV via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return _bronze_stream(
        "orders",
        "order_id STRING, customer_id STRING, order_date TIMESTAMP, status STRING, "
        "subtotal DOUBLE, shipping_cost DOUBLE, tax_amount DOUBLE, order_total DOUBLE, "
        "payment_method STRING, channel STRING",
    )


@dlt.table(
    name="bronze_order_items",
    comment="Raw order line items ingested from CSV via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_order_items():
    return _bronze_stream(
        "order_items",
        "item_id STRING, order_id STRING, product_id STRING, "
        "quantity INT, unit_price DOUBLE, discount_pct DOUBLE, line_total DOUBLE",
    )


# ─────────────────────────────────────────────────────────────────────────────
# SILVER — cleaned, deduplicated, validated
# DLT @expect decorators enforce data quality rules and surface metrics in UI
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="silver_customers",
    comment="Cleaned, deduplicated customer records",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_email",       "email IS NOT NULL AND email LIKE '%@%'")
def silver_customers():
    return (
        dlt.read("bronze_customers")
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("customer_id").orderBy(F.desc("_ingest_ts"))
        ))
        .filter("_rn = 1")
        .drop("_rn", "_source_file", "_ingest_ts")
        .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
        .withColumn("email",     F.lower(F.trim(F.col("email"))))
        .withColumn("_updated_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_products",
    comment="Cleaned product catalog with margin",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_product_id",  "product_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "unit_price > 0")   # drop rows with bad price
def silver_products():
    return (
        dlt.read("bronze_products")
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("product_id").orderBy(F.desc("_ingest_ts"))
        ))
        .filter("_rn = 1")
        .drop("_rn", "_source_file", "_ingest_ts")
        .withColumn(
            "margin_pct",
            F.round((F.col("unit_price") - F.col("cost_price")) / F.col("unit_price") * 100, 2)
        )
        .withColumn("_updated_at", F.current_timestamp())
    )


VALID_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]


@dlt.table(
    name="silver_orders",
    comment="Validated, enriched order headers",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_order_id",     "order_id IS NOT NULL")
@dlt.expect("valid_customer_id",  "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_total", "order_total > 0")
@dlt.expect_or_drop("valid_status",
                    "status IN ('pending','processing','shipped','delivered','cancelled','returned')")
def silver_orders():
    return (
        dlt.read("bronze_orders")
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.desc("_ingest_ts"))
        ))
        .filter("_rn = 1")
        .drop("_rn", "_source_file", "_ingest_ts")
        .withColumn("order_date",       F.to_timestamp("order_date"))
        .withColumn("order_year",       F.year("order_date"))
        .withColumn("order_month",      F.month("order_date"))
        .withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
        .withColumn("_updated_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_order_items",
    comment="Validated order line items with effective price",
    table_properties={"quality": "silver"},
)
@dlt.expect("valid_item_id",     "item_id IS NOT NULL")
@dlt.expect_or_drop("positive_qty", "quantity > 0")
def silver_order_items():
    return (
        dlt.read("bronze_order_items")
        .withColumn("_rn", F.row_number().over(
            Window.partitionBy("item_id").orderBy(F.desc("_ingest_ts"))
        ))
        .filter("_rn = 1")
        .drop("_rn", "_source_file", "_ingest_ts")
        .withColumn("effective_price",
                    F.round(F.col("unit_price") * (1 - F.col("discount_pct")), 2))
        .withColumn("_updated_at", F.current_timestamp())
    )


# ─────────────────────────────────────────────────────────────────────────────
# GOLD — business aggregates
# ─────────────────────────────────────────────────────────────────────────────

@dlt.table(
    name="gold_daily_sales",
    comment="Daily revenue and order counts by channel",
    table_properties={"quality": "gold"},
)
def gold_daily_sales():
    return (
        dlt.read("silver_orders")
        .filter("status != 'cancelled'")
        .groupBy(F.to_date("order_date").alias("order_date"), "channel")
        .agg(
            F.countDistinct("order_id").alias("num_orders"),
            F.round(F.sum("subtotal"),    2).alias("gross_revenue"),
            F.round(F.sum("order_total"), 2).alias("net_revenue"),
            F.round(F.avg("order_total"), 2).alias("avg_order_value"),
        )
        .orderBy("order_date")
    )


@dlt.table(
    name="gold_category_performance",
    comment="Monthly revenue and margin by product category",
    table_properties={"quality": "gold"},
)
def gold_category_performance():
    items = dlt.read("silver_order_items")
    prods = dlt.read("silver_products").select("product_id", "category", "cost_price")
    ords  = dlt.read("silver_orders").select("order_id", "order_year_month", "status")

    return (
        items
        .join(prods, "product_id", "left")
        .join(ords,  "order_id",   "left")
        .filter("status != 'cancelled'")
        .groupBy("order_year_month", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("line_total"), 2).alias("revenue"),
            F.round(F.sum(F.col("cost_price") * F.col("quantity")), 2).alias("total_cost"),
        )
        .withColumn("gross_margin", F.round(F.col("revenue") - F.col("total_cost"), 2))
        .withColumn("margin_pct",   F.round(F.col("gross_margin") / F.col("revenue") * 100, 2))
    )


@dlt.table(
    name="gold_customer_ltv",
    comment="Customer lifetime value and segmentation",
    table_properties={"quality": "gold"},
)
def gold_customer_ltv():
    ords  = dlt.read("silver_orders").filter("status = 'delivered'")
    custs = dlt.read("silver_customers").select("customer_id", "full_name", "city", "state", "is_premium")

    return (
        ords
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("order_total"), 2).alias("lifetime_revenue"),
            F.round(F.avg("order_total"), 2).alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .join(custs, "customer_id", "left")
        .withColumn(
            "clv_segment",
            F.when(F.col("lifetime_revenue") >= 1000, "High")
             .when(F.col("lifetime_revenue") >= 300,  "Mid")
             .otherwise("Low")
        )
    )
