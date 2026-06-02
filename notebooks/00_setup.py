# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog schemas (bronze / silver / gold) and uploads raw CSVs to DBFS.
# MAGIC
# MAGIC **Run this once before any other notebook.**

# COMMAND ----------

# MAGIC %md ## 1 · Config — edit these values

# COMMAND ----------

CATALOG = (
    "ecommerce_lakehouse"  # Unity Catalog catalog name (create in Data Explorer first)
)
RAW_PATH = "/Volumes/ecommerce_lakehouse/bronze/raw_data"  # where you uploaded the CSVs

# COMMAND ----------

# MAGIC %md ## 2 · Create catalog and schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")

for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"  schema ready: {CATALOG}.{schema}")

# COMMAND ----------

# MAGIC %md ## 3 · Verify raw files are in DBFS

# COMMAND ----------

expected = ["customers.csv", "products.csv", "orders.csv", "order_items.csv"]
files = [f.name for f in dbutils.fs.ls(RAW_PATH)]

for fname in expected:
    status = "OK" if fname in files else "MISSING"
    print(f"  {status}  {fname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Quick schema peek

# COMMAND ----------

for fname in expected:
    print(f"\n── {fname} ──")
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/{fname}")
    )
    df.printSchema()
    df.show(3, truncate=False)
