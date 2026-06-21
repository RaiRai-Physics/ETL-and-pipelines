# Enterprise Supply Chain ETL Project

An ETL project that simulates a real supply-chain data platform. It includes source landing zones, CDC-style data, contracts, bronze/silver/gold layers, quarantine handling, SCD Type 2 dimensions, facts, marts, reconciliation reports, exception reports, PySpark code, tests, SQL, and orchestration examples.

## Business Scenario

A multi-region distributor wants trusted analytics from ERP, OMS, WMS, TMS, procurement, invoicing, payments, and reference data. The pipeline creates curated warehouse-style data for sales, fulfillment, suppliers, warehouses, inventory, returns, and accounts receivable.

## Raw Source Areas

```text
data/raw/master/        customers, suppliers, products, warehouses, carriers
data/raw/transactions/  orders, lines, shipments, receipts, inventory, invoices, payments
data/raw/cdc/           product and customer CDC files
data/raw/reference/     calendar and FX rates
```

## Architecture

```text
Raw → Bronze → Silver → Gold
              ↘ Quarantine
```

## Advanced ETL Features

- Bronze, silver, gold architecture
- Data contracts
- Source profiling
- Standardized dates, timestamps, numerics, booleans, and text
- Primary key deduplication
- Foreign key validation
- Row quarantine with reason
- Currency conversion to USD
- Sales revenue, discount, cost, and margin logic
- Customer SCD Type 2 dimension
- Product SCD Type 2 dimension
- Fact tables and business marts
- Supplier scorecard
- Warehouse operations mart
- Fulfillment performance mart
- Customer 360 mart
- Accounts receivable aging
- Sales vs invoice reconciliation
- PO vs receipt reconciliation
- Exception tables
- Watermark metadata
- Audit logs
- PySpark implementation for core high-volume transformations
- Tests, SQL, Airflow DAG, Dockerfile, Makefile

## Run Python Pipeline

```bash
pip install -r requirements.txt
python src/python_etl/00_profile_sources.py
python src/python_etl/01_run_enterprise_etl.py
```

Or:

```bash
bash run_python_pipeline.sh
```

## Run PySpark Pipeline

```bash
python src/pyspark_etl/01_run_enterprise_pyspark_etl.py
```

## Run Tests

```bash
pytest
```


