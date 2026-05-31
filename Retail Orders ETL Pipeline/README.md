# Retail Complex ETL Project

A hands-on Python + PySpark ETL project with multiple raw CSV files, messy data, cleaning rules, joins, data quality checks, fact/dimension outputs, and analytics-ready reports.

## Business Scenario

Building an ETL pipeline for a retail company that sells products across stores and online channels. The company wants clean data and analytics reports for sales, returns, shipments, payments, and promotions.

## Raw Input Files

Located in:

```text
data/raw/
```

Files:

- `customers.csv`
- `products.csv`
- `stores.csv`
- `promotions.csv`
- `orders.csv`
- `order_items.csv`
- `payments.csv`
- `shipments.csv`
- `returns.csv`

## Intentional Data Issues

This project includes realistic ETL problems:

- Duplicate primary keys
- Missing values
- Mixed date formats
- Inconsistent casing
- Invalid numeric values
- Invalid foreign keys
- Unknown promo codes
- Expired promo codes
- Invalid quantities
- Missing payments
- Late shipments
- Returns and refunds

## Project Layers

```text
data/raw/             Original input CSVs
data/clean/python/    Cleaned CSVs from pandas pipeline
data/output/python/   Analytics outputs from pandas pipeline
data/clean/pyspark/   Cleaned CSV folders from PySpark pipeline
data/output/pyspark/  Analytics output folders from PySpark pipeline
reports/python/       Python profiling and DQ reports
reports/pyspark/      PySpark DQ reports
```

Important outputs:

- `dim_customers.csv`
- `dim_products.csv`
- `dim_stores.csv`
- `dim_promotions.csv`
- `fact_orders.csv`
- `fact_order_items.csv`
- `fact_payments.csv`
- `fact_shipments.csv`
- `fact_returns.csv`
- `sales_line_items_enriched.csv`
- `daily_sales_summary.csv`
- `category_revenue_summary.csv`
- `customer_lifetime_value.csv`
- `late_shipment_report.csv`
- `return_rate_by_product.csv`
- `payment_reconciliation_report.csv`
- `promotion_performance_report.csv`

## Skills Implemented:

- ETL project organization
- Raw data profiling
- Data quality reporting
- Cleaning messy CSV files
- Standardizing dates, numbers, booleans, and categories
- Removing duplicates
- Validating primary and foreign keys
- Building dimensions and fact tables
- Joining multiple datasets
- Creating analytics-ready summary outputs
- Translating pandas logic into PySpark logic

