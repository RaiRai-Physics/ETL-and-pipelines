# Advanced Financial ETL Project

This ETL simulates a banking/fintech data platform with raw operational data, cleaning, data quality checks, quarantine outputs, SCD Type 2 dimensions, currency conversion, fraud/risk reporting, customer 360 analytics, and audit metadata.

## Business Scenario

A financial services company wants a reliable ETL pipeline that combines core banking, card processor, loan servicing, fraud monitoring, and market exchange-rate data. The goal is to create clean warehouse-style tables and analytical marts for operations, risk, fraud, lending, and customer analytics.

## Raw Input CSVs

Located in:

```text
data/raw/
```

Included files:

- `branches.csv`
- `customers.csv`
- `customer_profile_updates.csv`
- `accounts.csv`
- `merchants.csv`
- `cards.csv`
- `exchange_rates.csv`
- `transactions.csv`
- `loans.csv`
- `loan_payments.csv`
- `credit_scores.csv`
- `fraud_alerts.csv`
- `chargebacks.csv`
- `account_daily_balances.csv`

## Pipeline Architecture

```text
Raw CSVs
   ↓
Profiling
   ↓
Clean Layer
   ↓
Data Quality Validation
   ↓
Quarantine Layer
   ↓
Warehouse Dimensions/Facts
   ↓
Analytics Marts
   ↓
Audit + Watermark Metadata
```

## Layers

```text
data/raw/                  Source CSVs
data/clean/python/         Standardized clean tables
data/quarantine/python/    Invalid or suspicious rows separated for review
data/marts/python/         Warehouse-style facts, dimensions, and analytics marts
reports/python/            Profiling, audit, and data quality reports
logs/run_metadata.json     Incremental watermark metadata
```

## Advanced ETL Concepts Included

- Data profiling
- Schema standardization
- Null handling
- Date/timestamp parsing
- Numeric casting
- Boolean normalization
- PII masking
- Deduplication
- Primary key validation
- Foreign key validation
- Quarantine handling
- Currency conversion to USD
- Signed debit/credit transaction logic
- SCD Type 2 customer dimension
- Fraud alert enrichment
- Chargeback analytics
- Loan delinquency reporting
- Card utilization reporting
- Account balance reconciliation
- Customer 360 mart
- Pipeline audit log
- Watermark metadata for incremental ETL
- PySpark version of core ETL

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run Python Pipeline

```bash
python src/python_etl/00_profile_raw_data.py
python src/python_etl/01_run_financial_python_etl.py
```

Or:

```bash
bash run_python_pipeline.sh
```

## Run PySpark Pipeline

```bash
python src/pyspark_etl/01_run_financial_pyspark_etl.py
```

Or:

```bash
bash run_pyspark_pipeline.sh
```

## Run Tests

```bash
pytest
```

## Important Outputs

Generated in `data/marts/python/`:

- `dim_customer_scd2.csv`
- `dim_customer_current.csv`
- `dim_account.csv`
- `dim_branch.csv`
- `dim_merchant.csv`
- `dim_card.csv`
- `dim_loan.csv`
- `fact_transactions.csv`
- `fact_loan_payments.csv`
- `fact_chargebacks.csv`
- `customer_360.csv`
- `daily_transaction_summary.csv`
- `merchant_risk_summary.csv`
- `suspicious_activity_report.csv`
- `loan_delinquency_report.csv`
- `card_utilization_report.csv`
- `account_balance_reconciliation.csv`
- `chargeback_summary.csv`

