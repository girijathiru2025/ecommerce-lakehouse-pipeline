# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 · Gold Layer — Business Aggregates
# MAGIC
# MAGIC Produces five business-ready tables from silver:
# MAGIC
# MAGIC | Gold table                  | Description                                      |
# MAGIC |-----------------------------|--------------------------------------------------|
# MAGIC | `daily_sales_summary`       | Revenue, orders, units by day                    |
# MAGIC | `category_performance`      | Revenue & margin by product category × month     |
# MAGIC | `customer_lifetime_value`   | CLV, order frequency, avg basket per customer    |
# MAGIC | `order_funnel`              | Order counts by status (funnel/cancellation rate)|
# MAGIC | `top_products`              | Top 20 products by revenue                       |

# COMMAND ----------

CATALOG = "ecommerce_lakehouse"

from pyspark.sql import functions as F


def overwrite_gold(table_name: str, df):
    """Replace gold table fully on each run (aggregates are always recomputed)."""
    full = f"{CATALOG}.gold.{table_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full)
    print(f"  gold.{table_name}  ({df.count():,} rows)")


orders      = spark.table(f"{CATALOG}.silver.orders")
order_items = spark.table(f"{CATALOG}.silver.order_items")
products    = spark.table(f"{CATALOG}.silver.products")
customers   = spark.table(f"{CATALOG}.silver.customers")

# COMMAND ----------

# MAGIC %md ## 1 · Daily Sales Summary

# COMMAND ----------

daily_sales = (
    orders
    .filter("status != 'cancelled'")
    .groupBy(F.to_date("order_date").alias("order_date"), "channel")
    .agg(
        F.countDistinct("order_id").alias("num_orders"),
        F.round(F.sum("subtotal"),  2).alias("gross_revenue"),
        F.round(F.sum("order_total"), 2).alias("net_revenue"),
        F.round(F.avg("order_total"), 2).alias("avg_order_value"),
        F.round(F.sum("shipping_cost"), 2).alias("total_shipping"),
        F.round(F.sum("tax_amount"), 2).alias("total_tax"),
    )
    .orderBy("order_date", "channel")
)

overwrite_gold("daily_sales_summary", daily_sales)

# COMMAND ----------

# MAGIC %md ## 2 · Category Performance (monthly)

# COMMAND ----------

items_enriched = (
    order_items
    .join(products.select("product_id", "category", "cost_price"), "product_id", "left")
    .join(orders.select("order_id", "order_year_month", "status"), "order_id", "left")
    .filter("status != 'cancelled'")
)

category_perf = (
    items_enriched
    .groupBy("order_year_month", "category")
    .agg(
        F.count("item_id").alias("num_line_items"),
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("revenue"),
        F.round(F.sum(F.col("cost_price") * F.col("quantity")), 2).alias("total_cost"),
    )
    .withColumn("gross_margin", F.round(F.col("revenue") - F.col("total_cost"), 2))
    .withColumn("margin_pct",   F.round(F.col("gross_margin") / F.col("revenue") * 100, 2))
    .orderBy("order_year_month", F.desc("revenue"))
)

overwrite_gold("category_performance", category_perf)

# COMMAND ----------

# MAGIC %md ## 3 · Customer Lifetime Value

# COMMAND ----------

customer_orders = (
    orders
    .filter("status = 'delivered'")
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("order_total"), 2).alias("lifetime_revenue"),
        F.round(F.avg("order_total"), 2).alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    .withColumn(
        "days_active",
        F.datediff(F.col("last_order_date"), F.col("first_order_date"))
    )
    .withColumn(
        "orders_per_month",
        F.round(F.col("total_orders") / F.greatest(F.col("days_active") / 30, F.lit(1)), 2)
    )
)

clv = (
    customer_orders
    .join(customers.select("customer_id", "full_name", "city", "state", "is_premium"), "customer_id", "left")
    .withColumn(
        "clv_segment",
        F.when(F.col("lifetime_revenue") >= 1000, "High")
         .when(F.col("lifetime_revenue") >= 300,  "Mid")
         .otherwise("Low")
    )
    .orderBy(F.desc("lifetime_revenue"))
)

overwrite_gold("customer_lifetime_value", clv)

# COMMAND ----------

# MAGIC %md ## 4 · Order Funnel / Status Distribution

# COMMAND ----------

total_orders = orders.count()

funnel = (
    orders
    .groupBy("status")
    .agg(F.count("order_id").alias("num_orders"))
    .withColumn("pct_of_total", F.round(F.col("num_orders") / total_orders * 100, 2))
    .orderBy(F.desc("num_orders"))
)

overwrite_gold("order_funnel", funnel)

# COMMAND ----------

# MAGIC %md ## 5 · Top Products by Revenue

# COMMAND ----------

top_products = (
    order_items.select("order_id", "product_id", "quantity", "line_total")
    .join(orders.select("order_id", "status"), "order_id", "left")
    .filter("status != 'cancelled'")
    .join(products.select("product_id", "product_name", "category", F.col("unit_price").alias("list_price")), "product_id", "left")
    .groupBy("product_id", "product_name", "category", "list_price")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.round(F.sum("line_total"), 2).alias("total_revenue"),
        F.countDistinct("order_id").alias("num_orders"),
    )
    .orderBy(F.desc("total_revenue"))
    .limit(20)
    .withColumn("revenue_rank", F.monotonically_increasing_id() + 1)
    .withColumnRenamed("list_price", "unit_price")
)

overwrite_gold("top_products", top_products)

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

for tbl in ("daily_sales_summary", "category_performance", "customer_lifetime_value", "order_funnel", "top_products"):
    count = spark.table(f"{CATALOG}.gold.{tbl}").count()
    print(f"  gold.{tbl:30s}  {count:>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC **Next:** open a SQL Warehouse and run queries against `ecommerce_lakehouse.gold.*`
# MAGIC
# MAGIC Example:
# MAGIC ```sql
# MAGIC SELECT order_year_month, SUM(revenue) AS monthly_revenue
# MAGIC FROM ecommerce_lakehouse.gold.category_performance
# MAGIC GROUP BY 1 ORDER BY 1;
# MAGIC ```
