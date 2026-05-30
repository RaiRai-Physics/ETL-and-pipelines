# Retail ETL Project Input Data

This folder contains raw CSV input files for the retail orders ETL pipeline

## Files

- customers.csv
- products.csv
- stores.csv
- promotions.csv
- orders.csv
- order_items.csv
- payments.csv
- shipments.csv
- returns.csv

## Intentional Data Issues

The files include the following problems:
- duplicate primary keys
- missing values
- inconsistent casing
- mixed date formats
- invalid numeric values
- invalid foreign keys
- inconsistent status values
- returns and refunds
- expired and unknown promo codes

## ETL Layers

- data/raw: original CSV files
- data/clean: standardized and deduplicated data
- data/output: analytics-ready facts, dimensions, and reports
