# Smart Grid & Renewable Energy ETL Platform

A production-style ETL project for utility and renewable-energy data. It processes AMI meter readings, SCADA asset telemetry, generation, weather, outages, maintenance, demand-response events, bills, payments, market prices, and CDC feeds.

## Architecture

```text
Raw sources -> Bronze -> Silver -> Quarantine -> Gold facts/dimensions/marts -> Audit & watermarks
```

## Main concepts

- High-volume time-series ETL
- Bronze, silver, gold layers
- Data contracts and quarantine
- Customer and asset SCD Type 2
- Meter consumption and peak demand
- Renewable generation and curtailment
- Weather enrichment
- Forecast accuracy and exception flags
- Asset telemetry anomaly detection
- Outage reliability metrics
- Maintenance performance
- Demand-response performance
- Billing, collections, and AR aging
- Customer 360
- Grid supply-demand balance
- Energy market price enrichment
- Incremental watermarks
- Python and PySpark implementations

## Run

```bash
pip install -r requirements.txt
python src/python_etl/00_profile_sources.py
python src/python_etl/01_run_smart_grid_etl.py
```

PySpark:

```bash
python src/pyspark_etl/01_run_smart_grid_pyspark_etl.py
```

Tests:

```bash
pytest
```
