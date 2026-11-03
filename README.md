# E-Commerce Lakehouse Pipeline

End-to-end Databricks ETL project using **Auto Loader, Delta Lake, and PySpark** with the **Medallion Architecture (Bronze / Silver / Gold)**.

Built as a self-directed learning project on synthetic data. The pipeline runs as three notebooks (Bronze, Silver, Gold). An alternative Delta Live Tables version is included but has not been run (see "Optional: Delta Live Tables version" below).

## Architecture

```
CSV files (Unity Catalog Volume)
      │
      ▼
 Auto Loader          ← incremental ingest, schema evolution
      │
      ▼
 Bronze Layer         ← raw append-only Delta tables + ingest metadata
      │
      ▼
 Silver Layer         ← MERGE upserts, deduplication, null checks, derived columns
      │
      ▼
 Gold Layer           ← 5 business-ready aggregate tables
```

## Dataset

Synthetic e-commerce data, generated locally with no external dependency:

| File            | Rows    | Description       |
|-----------------|---------|-------------------|
| customers.csv   | 2,000   | Customer profiles |
| products.csv    | 200     | Product catalog   |
| orders.csv      | 15,000  | Order headers     |
| order_items.csv | ~50,000 | Order line items  |

## Gold Tables

| Table                     | Description                                  |
|---------------------------|----------------------------------------------|
| `daily_sales_summary`     | Revenue & orders by day x channel            |
| `category_performance`    | Revenue & margin by category x month         |
| `customer_lifetime_value` | CLV, order frequency, segment (High/Mid/Low) |
| `order_funnel`            | Order counts by status                       |
| `top_products`            | Top 20 products by revenue                   |

## Project Structure

```
ecommerce-lakehouse-pipeline/
├── data_generator/
│   └── generate_sample_data.py   # run locally to produce CSVs
├── data/                         # generated CSVs (gitignored)
├── notebooks/
│   ├── 00_setup.py               # create Unity Catalog schemas, verify data
│   ├── 01_bronze_ingestion.py    # Auto Loader -> bronze Delta tables
│   ├── 02_silver_transforms.py   # clean/dedupe/validate -> silver (MERGE upsert)
│   └── 03_gold_aggregates.py     # business aggregates -> gold
└── dlt_pipeline/
    └── ecommerce_dlt_pipeline.py # optional Delta Live Tables version (not yet run)
```

## Setup & Run

### Step 1: Generate sample data

```
cd ecommerce-lakehouse-pipeline
python data_generator/generate_sample_data.py
```

### Step 2: Upload the CSVs to your Databricks workspace

Upload the files in `data/` to a Unity Catalog Volume in your workspace.

### Step 3: Create the catalog

In Databricks UI -> Data -> Create Catalog -> name it `ecommerce_lakehouse`

### Step 4: Run the notebooks in order

Import all notebooks from `notebooks/` into your workspace and run:

1. `00_setup.py`
2. `01_bronze_ingestion.py`
3. `02_silver_transforms.py`
4. `03_gold_aggregates.py`

## Optional: Delta Live Tables version

`dlt_pipeline/ecommerce_dlt_pipeline.py` implements the same pipeline as a Delta Live Tables pipeline with inline `@dlt.expect` data-quality rules. **I have written this version but not run it yet**, so it is untested.

To try it: Workflows -> Delta Live Tables -> Create Pipeline, point it at the file, set the `raw_path` config to your Volume path, and start it.

## Databricks Concepts Used

- **Auto Loader** (`cloudFiles`): incremental file ingestion with schema evolution
- **Delta Lake**: ACID transactions and MERGE upserts
- **Medallion Architecture**: Bronze / Silver / Gold layering
- **Unity Catalog**: catalog and schema organization
- **PySpark**: transformations and aggregations