# E-Commerce Lakehouse Pipeline

End-to-end Databricks ETL project using **Auto Loader → Delta Live Tables → Medallion Architecture (Bronze / Silver / Gold)**.

## Architecture

```
CSV files (DBFS)
      │
      ▼
 Auto Loader          ← incremental ingest, schema evolution
      │
      ▼
 Bronze Layer         ← raw append-only Delta tables + ingest metadata
      │
      ▼
 Silver Layer         ← cleaned, typed, deduplicated, DLT expectations
      │
      ▼
 Gold Layer           ← 5 business-ready aggregate tables
      │
      ▼
 SQL Warehouse        ← BI / ad-hoc queries
```

## Dataset

Synthetic e-commerce data (generated, no external dependency):

| File              | Rows   | Description                        |
|-------------------|--------|------------------------------------|
| customers.csv     | 2,000  | Customer profiles                  |
| products.csv      | 200    | Product catalog                    |
| orders.csv        | 15,000 | Order headers                      |
| order_items.csv   | ~50,000| Order line items                   |

## Gold Tables

| Table                     | Description                                    |
|---------------------------|------------------------------------------------|
| `daily_sales_summary`     | Revenue & orders by day × channel              |
| `category_performance`    | Revenue & margin by category × month           |
| `customer_lifetime_value` | CLV, order frequency, segment (High/Mid/Low)   |
| `order_funnel`            | Order counts by status                         |
| `top_products`            | Top 20 products by revenue                     |

## Project Structure

```
ecommerce-lakehouse-pipeline/
├── data_generator/
│   └── generate_sample_data.py   # run locally to produce CSVs
├── data/                         # generated CSVs (gitignored)
├── notebooks/
│   ├── 00_setup.py               # create Unity Catalog schemas, verify data
│   ├── 01_bronze_ingestion.py    # Auto Loader → bronze Delta tables
│   ├── 02_silver_transforms.py   # clean/dedupe/validate → silver (MERGE upsert)
│   └── 03_gold_aggregates.py     # business aggregates → gold
└── dlt_pipeline/
    └── ecommerce_dlt_pipeline.py # full pipeline as Delta Live Tables
```

## Setup & Run

### Step 1 — Generate sample data

```bash
cd ecommerce-lakehouse-pipeline
python data_generator/generate_sample_data.py
```

### Step 2 — Upload to DBFS

```bash
# Install Databricks CLI first: pip install databricks-cli
databricks configure --token
databricks fs mkdirs dbfs:/FileStore/ecommerce/raw
databricks fs cp -r data/ dbfs:/FileStore/ecommerce/raw/
```

### Step 3 — Create Unity Catalog

In Databricks UI → Data → Create Catalog → name it `ecommerce_lakehouse`

### Step 4 — Run notebooks in order

Import all notebooks from `notebooks/` into your Databricks workspace and run:
1. `00_setup.py`
2. `01_bronze_ingestion.py`
3. `02_silver_transforms.py`
4. `03_gold_aggregates.py`

### Step 5 (optional) — Run as Delta Live Tables pipeline

1. Go to **Workflows → Delta Live Tables → Create Pipeline**
2. Point to `dlt_pipeline/ecommerce_dlt_pipeline.py`
3. Set pipeline config: `raw_path` = `dbfs:/FileStore/ecommerce/raw`
4. Click **Start**

The DLT pipeline does everything notebooks 01–03 do, but with built-in data quality metrics visible in the UI.

## Key Databricks Concepts Demonstrated

- **Auto Loader** (`cloudFiles`) — incremental file ingestion with schema evolution
- **Delta Live Tables** — declarative pipeline with `@dlt.expect` data quality rules
- **Delta Lake** — ACID transactions, MERGE upserts, time travel
- **Medallion Architecture** — Bronze / Silver / Gold layering
- **Unity Catalog** — catalog + schema governance
- **SQL Warehouse** — query gold tables from BI tools
