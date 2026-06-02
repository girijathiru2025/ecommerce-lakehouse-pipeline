# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 · Silver Layer — Clean & Conform
# MAGIC
# MAGIC Reads from bronze tables, applies:
# MAGIC - Type casting & null handling
# MAGIC - Deduplication (by primary key, keep latest ingest)
# MAGIC - Bad-row filtering with quarantine
# MAGIC - Derived columns (full_name, order_year_month, margin_pct)
# MAGIC
# MAGIC Writes using **MERGE** (upsert) so re-runs are idempotent.

# COMMAND ----------

CATALOG = "ecommerce_lakehouse"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from delta.tables import DeltaTable


def upsert_silver(target_table: str, df: DataFrame, pk: str):
    """MERGE incoming df into silver target on primary key."""
    full_name = f"{CATALOG}.silver.{target_table}"

    if not spark.catalog.tableExists(full_name):
        df.write.format("delta").saveAsTable(full_name)
        print(f"  created  silver.{target_table}")
        return

    dt = DeltaTable.forName(spark, full_name)
    (
        dt.alias("t")
          .merge(df.alias("s"), f"t.{pk} = s.{pk}")
          .whenMatchedUpdateAll()
          .whenNotMatchedInsertAll()
          .execute()
    )
    print(f"  upserted silver.{target_table}")

# COMMAND ----------

# MAGIC %md ## Silver: customers

# COMMAND ----------

customers_silver = (
    spark.table(f"{CATALOG}.bronze.customers")
    # dedupe: keep row with latest ingest timestamp per customer_id
    .withColumn("_rn", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("customer_id").orderBy(F.desc("_ingest_ts"))
    ))
    .filter("_rn = 1")
    .drop("_rn", "_source_file", "_ingest_ts")
    # null guard
    .filter(F.col("customer_id").isNotNull() & F.col("email").isNotNull())
    # derived
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("_updated_at", F.current_timestamp())
)

upsert_silver("customers", customers_silver, "customer_id")

# COMMAND ----------

# MAGIC %md ## Silver: products

# COMMAND ----------

products_silver = (
    spark.table(f"{CATALOG}.bronze.products")
    .withColumn("_rn", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("product_id").orderBy(F.desc("_ingest_ts"))
    ))
    .filter("_rn = 1")
    .drop("_rn", "_source_file", "_ingest_ts")
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("unit_price") > 0)        # reject zero/negative prices
    .withColumn(
        "margin_pct",
        F.round((F.col("unit_price") - F.col("cost_price")) / F.col("unit_price") * 100, 2)
    )
    .withColumn("_updated_at", F.current_timestamp())
)

upsert_silver("products", products_silver, "product_id")

# COMMAND ----------

# MAGIC %md ## Silver: orders

# COMMAND ----------

VALID_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]

orders_raw = spark.table(f"{CATALOG}.bronze.orders")

# quarantine bad rows before writing silver
bad_orders = (
    orders_raw
    .filter(
        F.col("order_id").isNull() |
        F.col("customer_id").isNull() |
        ~F.col("status").isin(VALID_STATUSES) |
        (F.col("order_total") <= 0)
    )
    .withColumn("_quarantine_reason", F.lit("failed silver validation"))
    .withColumn("_quarantine_ts", F.current_timestamp())
)

if bad_orders.count() > 0:
    bad_orders.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.silver.orders_quarantine")
    print(f"  quarantined {bad_orders.count()} bad order rows")

orders_silver = (
    orders_raw
    .filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        F.col("status").isin(VALID_STATUSES) &
        (F.col("order_total") > 0)
    )
    .withColumn("_rn", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("order_id").orderBy(F.desc("_ingest_ts"))
    ))
    .filter("_rn = 1")
    .drop("_rn", "_source_file", "_ingest_ts")
    .withColumn("order_date", F.to_timestamp("order_date"))
    .withColumn("order_year",       F.year("order_date"))
    .withColumn("order_month",      F.month("order_date"))
    .withColumn("order_year_month", F.date_format("order_date", "yyyy-MM"))
    .withColumn("_updated_at", F.current_timestamp())
)

upsert_silver("orders", orders_silver, "order_id")

# COMMAND ----------

# MAGIC %md ## Silver: order_items

# COMMAND ----------

order_items_silver = (
    spark.table(f"{CATALOG}.bronze.order_items")
    .withColumn("_rn", F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("item_id").orderBy(F.desc("_ingest_ts"))
    ))
    .filter("_rn = 1")
    .drop("_rn", "_source_file", "_ingest_ts")
    .filter(F.col("item_id").isNotNull() & (F.col("quantity") > 0))
    .withColumn("effective_price", F.round(F.col("unit_price") * (1 - F.col("discount_pct")), 2))
    .withColumn("_updated_at", F.current_timestamp())
)

upsert_silver("order_items", order_items_silver, "item_id")

# COMMAND ----------

# MAGIC %md ## Verify

# COMMAND ----------

for tbl in ("customers", "products", "orders", "order_items"):
    count = spark.table(f"{CATALOG}.silver.{tbl}").count()
    print(f"  silver.{tbl:15s}  {count:>10,} rows")
