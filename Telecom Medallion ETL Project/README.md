# Telecom 5G Medallion ETL Project

A fully synthetic ETL project built explicitly around the **Medallion Architecture**.

## Business Scenario
A telecom operator needs a trusted analytics platform combining subscriber/customer master data, devices, plans, voice calls, mobile-data sessions, SMS events, cell-tower KPIs, alarms, outages, invoices, payments, support tickets, and CDC feeds.

## Medallion Architecture
```text
Raw landing zone
   ↓
Bronze: immutable source copies + ingestion metadata/lineage
   ↓
Silver: typed, standardized, deduplicated, validated data
   ├── Quarantine: rows failing business/data-quality rules
   ↓
Gold: facts, SCD2 dimensions, KPI marts, finance marts, risk/exception outputs
```

## Major Gold Outputs
- dim_customer_scd2 / dim_customer_current
- dim_plan_scd2 / dim_plan_current
- dim_subscriber, dim_device, dim_site, dim_cell
- fact_voice_calls, fact_data_sessions, fact_sms_events
- fact_cell_kpis, fact_network_alarms, fact_site_outages
- fact_invoices, fact_payments, fact_support_tickets
- mart_subscriber_monthly_usage
- mart_cell_network_health
- mart_site_reliability
- mart_accounts_receivable
- mart_monthly_revenue
- mart_customer_360_churn_risk
- mart_plan_performance
- mart_device_5g_adoption
- mart_support_operations
- exception marts for high data use, poor QoE, dropped calls, network utilization, availability, past-due accounts, churn risk, and critical alarms

## Run
```bash
pip install -r requirements.txt
python src/python_etl/00_profile_sources.py
python src/python_etl/01_run_medallion_etl.py
```
PySpark core pipeline:
```bash
python src/pyspark_etl/01_run_medallion_pyspark_etl.py
```
Tests:
```bash
pytest -q
```

## Summary
Built a telecom 5G medallion ETL platform using Python, PySpark, and SQL to process ~300K synthetic usage, network, billing, and support records; implemented Bronze lineage, Silver standardization and quarantine, SCD Type 2 dimensions, network QoE analytics, revenue/AR marts, customer churn-risk scoring, and incremental watermarks.
