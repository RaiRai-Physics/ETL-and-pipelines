# Walmart Retail ETL Project

This is a fully synthetic project inspired by retail transaction patterns. It is **not official Walmart data** and does not represent Walmart internal systems.

## Business Scenario

Build a trusted retail data platform that combines:

- Store POS transactions
- E-commerce orders
- Split-tender payments
- Product and supplier master data
- Customer loyalty data
- Promotions and markdowns
- Returns
- Store inventory snapshots
- Inventory adjustments
- Product-price CDC
- Customer-loyalty CDC

## Architecture

```text
Raw sources
   ↓
Bronze
   ↓
Silver
   ├── Data-quality quarantine
   ↓
Gold dimensional model
   ↓
Omnichannel marts + exceptions + reconciliation
```

## Scale

The project contains 18 raw CSV datasets and tens of thousands of transaction-level rows. 

## Key Engineering Features

- Bronze / Silver / Gold architecture
- Config-driven thresholds
- Data contracts
- Raw data profiling
- Deduplication
- Primary and foreign-key validation
- Bad-row quarantine with reason
- CDC feeds
- Customer SCD Type 2
- Product/price SCD Type 2
- POS line-level fact
- Online sales fact
- Store and online omnichannel aggregation
- Payment reconciliation
- Returns and return-risk analysis
- Promotion effectiveness
- Markdown analysis
- Inventory health and stockout detection
- Carrier delivery performance
- Customer 360
- High-value basket exception report
- Audit logging
- Incremental watermarks
- Python and PySpark pipelines
- SQL, tests, Airflow example, Dockerfile

## Run Python Pipeline

```bash
pip install -r requirements.txt
python src/python_etl/00_profile_sources.py
python src/python_etl/01_run_retail_etl.py
```

Or:

```bash
bash run_python_pipeline.sh
```

## Run PySpark Core Pipeline

```bash
python src/pyspark_etl/01_run_retail_pyspark_etl.py
```

## Run Tests

```bash
pytest
```

## Important Gold Outputs

- `dim_customer_scd2.csv`
- `dim_product_scd2.csv`
- `fact_pos_sales_lines.csv`
- `fact_pos_transactions.csv`
- `fact_online_sales_lines.csv`
- `fact_returns.csv`
- `inventory_position_current.csv`
- `mart_daily_store_sales.csv`
- `mart_daily_online_sales.csv`
- `mart_omnichannel_store_performance.csv`
- `mart_product_performance.csv`
- `mart_customer_360.csv`
- `mart_promotion_performance.csv`
- `mart_store_inventory_health.csv`
- `mart_carrier_performance.csv`
- `mart_return_risk.csv`
- `recon_pos_sales_vs_payments.csv`
- `exception reports for payment mismatches, stockouts, high-value baskets, suspicious returns, and late shipments.`
  

