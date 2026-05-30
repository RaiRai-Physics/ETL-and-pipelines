retail_complex_etl_project/
├── .gitignore
├── README.md
├── data/
│   ├── clean/
│   │   ├── python/
│   │   │   ├── customers.csv
│   │   │   ├── order_items.csv
│   │   │   ├── orders.csv
│   │   │   ├── payments.csv
│   │   │   ├── products.csv
│   │   │   ├── promotions.csv
│   │   │   ├── returns.csv
│   │   │   ├── shipments.csv
│   │   │   ├── stores.csv
│   ├── output/
│   │   ├── python/
│   │   │   ├── category_revenue_summary.csv
│   │   │   ├── customer_lifetime_value.csv
│   │   │   ├── daily_sales_summary.csv
│   │   │   ├── dim_customers.csv
│   │   │   ├── dim_products.csv
│   │   │   ├── dim_promotions.csv
│   │   │   ├── dim_stores.csv
│   │   │   ├── fact_order_items.csv
│   │   │   ├── fact_orders.csv
│   │   │   ├── fact_orders_enriched.csv
│   │   │   ├── fact_payments.csv
│   │   │   ├── fact_returns.csv
│   │   │   ├── fact_shipments.csv
│   │   │   ├── late_shipment_report.csv
│   │   │   ├── payment_reconciliation_report.csv
│   │   │   ├── promotion_performance_report.csv
│   │   │   ├── return_rate_by_product.csv
│   │   │   ├── sales_line_items_enriched.csv
│   ├── raw/
│   │   ├── customers.csv
│   │   ├── order_items.csv
│   │   ├── orders.csv
│   │   ├── payments.csv
│   │   ├── products.csv
│   │   ├── promotions.csv
│   │   ├── returns.csv
│   │   ├── shipments.csv
│   │   ├── stores.csv
├── docs/
│   ├── data_dictionary.md
│   ├── data_quality_rules.md
├── reports/
│   ├── python/
│   │   ├── data_quality_checks.csv
│   │   ├── raw_column_profile.csv
│   │   ├── raw_table_profile.csv
├── requirements.txt
├── sql/
│   ├── analytics_queries.sql
├── src/
│   ├── pyspark_etl/
│   │   ├── 01_run_retail_pyspark_etl.py
│   ├── python_etl/
│   │   ├── 00_profile_raw_data.py
│   │   ├── 01_run_retail_python_etl.py
