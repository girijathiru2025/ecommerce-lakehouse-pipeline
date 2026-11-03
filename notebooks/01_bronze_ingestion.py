# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 · Bronze Layer — Auto Loader Ingestion
# MAGIC
# MAGIC Reads raw CSVs incrementally from a Unity Catalog Volume using **Auto Loader** (`cloudFiles`) and writes
# MAGIC append-only Delta tables in the bronze schema.  No business logic — raw data lands as-is,
# MAGIC plus ingest metadata columns for lineage.
# MAGIC
# MAGIC | Source file        | Bronze table                        |
# MAGIC |--------------------|-------------------------------------|
# MAGIC | customers.csv      | ecommerce_lakehouse.bronze.customers |
# MAGIC | products.csv       | ecommerce_lakehouse.bronze.products  |
# MAGIC | orders.csv         | ecommerce_lakehouse.bronze.orders    |
# MAGIC | order_items.csv    | ecommerce_lakehouse.bronze.order_items |

# COMMAND ----------

# MAGIC %md ## Config

# COMMAND ----------

CATALOG = "ecommerce_lakehouse"
RAW_PATH = "/Volumes/ecommerce_lakehouse/bronze/raw_data"
CHECKPOINT_BASE = "/Volumes/ecommerce_lakehouse/bronze/raw_data/_checkpoints"

# COMMAND ----------

# MAGIC %md ## Helper — run Auto Loader stream for one entity

# COMMAND ----------

from pyspark.sql import functions as F


def ingest_bronze(entity: str, schema_hint: str = None):
    """
    Reads new files from RAW_PATH/<entity>.csv using Auto Loader.
    Writes (append) to bronze.<entity> Delta table.
    schema_hint: optional DDL string to avoid full schema inference on every run.
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/{entity}/schema")
        .option("pathGlobFilter", f"{entity}.csv")
        .option("header", "true")
        .option("inferSchema", "true" if schema_hint is None else "false")
    )

    if schema_hint:
        reader = reader.schema(schema_hint)

    df = (
        reader.load(RAW_PATH)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

    query = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{entity}/ckpt")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # batch-style: drain all new files then stop
        .toTable(f"{CATALOG}.bronze.{entity}")
    )
    query.awaitTermination()
    print(f"  bronze.{entity}  ingested")


# COMMAND ----------

# MAGIC %md ## Ingest all four entities

# COMMAND ----------

# Explicit schemas speed up Auto Loader and prevent type mis-inference on re-runs
SCHEMAS = {
    "customers": """
        customer_id STRING, first_name STRING, last_name STRING, email STRING,
        city STRING, state STRING, country STRING,
        signup_date TIMESTAMP, is_premium BOOLEAN
    """,
    "products": """
        product_id STRING, product_name STRING, category STRING,
        unit_price DOUBLE, cost_price DOUBLE, stock_qty INT, is_active BOOLEAN
    """,
    "orders": """
        order_id STRING, customer_id STRING, order_date TIMESTAMP, status STRING,
        subtotal DOUBLE, shipping_cost DOUBLE, tax_amount DOUBLE, order_total DOUBLE,
        payment_method STRING, channel STRING
    """,
    "order_items": """
        item_id STRING, order_id STRING, product_id STRING,
        quantity INT, unit_price DOUBLE, discount_pct DOUBLE, line_total DOUBLE
    """,
}

for entity, schema in SCHEMAS.items():
    ingest_bronze(entity, schema_hint=schema.strip())

# COMMAND ----------

# MAGIC %md ## Verify row counts

# COMMAND ----------

for entity in SCHEMAS:
    count = spark.table(f"{CATALOG}.bronze.{entity}").count()
    print(f"  bronze.{entity:15s}  {count:>10,} rows")
