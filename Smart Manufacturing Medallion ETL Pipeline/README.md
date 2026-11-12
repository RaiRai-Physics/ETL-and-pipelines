# Smart Manufacturing + Industrial IoT Medallion ETL

A synthetic, production-style manufacturing data engineering project using a true Bronze / Silver / Gold Medallion architecture.

## Business scenario

A multi-plant manufacturer wants one trusted data platform combining ERP work orders, MES production, SCADA telemetry, downtime, quality, maintenance, supplier receipts, inventory, energy usage, and CDC feeds.

## Architecture

```text
Raw Landing
   ↓
Bronze
   - source-preserving copies
   - ingestion timestamp
   - source-file lineage
   ↓
Silver
   - schema normalization
   - dates/timestamps/numerics
   - deduplication
   - PK/FK validation
   - telemetry anomaly flags
   ├────────────→ Quarantine
   ↓
Gold
   - SCD2 dimensions
   - facts
   - OEE / reliability / quality marts
   - reconciliation tables
   - exception marts
```

## Major engineering features

- 19 synthetic source datasets
- high-volume industrial telemetry
- Bronze source lineage
- Silver validation and standardization
- Quarantine with explicit rejection reasons
- Machine SCD Type 2
- Product cost/status SCD Type 2
- OEE calculation
- MTBF and MTTR
- machine anomaly detection
- production scrap and rework analytics
- product quality scorecards
- supplier fill-rate and reject-rate analysis
- inventory stockout/safety-stock risk
- energy intensity and renewable share
- work-order vs production reconciliation
- maintenance backlog
- config-driven exception thresholds
- audit logs and incremental watermarks
- Python and PySpark implementations
- SQL, Airflow, Docker, pytest

## Run Python pipeline

```bash
pip install -r requirements.txt
python src/python_etl/00_profile_sources.py
python src/python_etl/01_run_medallion_etl.py
```

Or:

```bash
bash run_python_pipeline.sh
```

## Run PySpark

```bash
python src/pyspark_etl/01_run_medallion_pyspark_etl.py
```

## Run tests

```bash
pytest -q
```

## Key Gold outputs

- dim_machine_scd2.csv
- dim_product_scd2.csv
- fact_production_runs.csv
- fact_machine_telemetry.csv
- fact_downtime.csv
- fact_quality_inspections.csv
- fact_maintenance_orders.csv
- mart_daily_line_oee.csv
- mart_machine_health.csv
- mart_machine_reliability.csv
- mart_product_quality.csv
- mart_supplier_scorecard.csv
- mart_inventory_risk.csv
- mart_energy_intensity.csv
- mart_plant_scorecard.csv
- mart_maintenance_backlog.csv
- recon_work_orders_vs_production.csv


