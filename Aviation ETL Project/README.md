# Aviation Operations + Revenue Medallion ETL Project

A synthetic, portfolio-ready airline data platform built around a real Bronze → Silver → Gold Medallion architecture.

## Scenario

An airline needs a trusted analytics platform combining:
- flight schedules and actual operations
- bookings, tickets, fare classes and baggage
- loyalty and passenger data
- payment and refund activity
- crew assignments
- aircraft fleet and maintenance events
- routes, airports and weather

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
    - data type enforcement
    - standardization
    - deduplication
    - PK/FK validation
    - timestamp parsing
    ├────────→ Quarantine
    │          - missing references
    │          - bad numerics
    │          - bad relationships
    ↓
Gold
    - SCD2 dimensions
    - fact tables
    - operational marts
    - revenue/reconciliation marts
    - exception marts
```

## Raw Datasets

21 CSV sources across master, booking, operations, revenue, maintenance, CDC and reference domains.

## Advanced Engineering Concepts

- Bronze / Silver / Gold Medallion architecture
- data contracts
- source profiling
- data lineage metadata
- deduplication
- PK/FK validation
- quarantine handling
- Loyalty SCD Type 2
- Aircraft SCD Type 2
- flight operations facts
- booking-segment facts
- baggage facts
- maintenance facts
- payment reconciliation
- refund analysis
- route performance
- load factor
- on-time performance
- cancellation rate
- aircraft reliability
- baggage mishandling
- crew workload
- passenger 360
- loyalty performance
- airport operations
- incremental watermarks
- pipeline audit reporting
- Python + PySpark implementations
- SQL, Airflow, Docker, pytest

## Run Python Pipeline

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

## Run Tests

```bash
pytest -q
```

## Key Gold Outputs

- `dim_loyalty_scd2.csv`
- `dim_aircraft_scd2.csv`
- `fact_flights.csv`
- `fact_booking_segments.csv`
- `fact_baggage.csv`
- `fact_maintenance.csv`
- `fact_booking_revenue.csv`
- `mart_flight_performance.csv`
- `mart_route_performance.csv`
- `mart_baggage_performance.csv`
- `mart_aircraft_reliability.csv`
- `mart_crew_workload.csv`
- `mart_passenger_360.csv`
- `mart_loyalty_performance.csv`
- `mart_airport_operations.csv`
- `mart_refund_risk.csv`
- `recon_bookings_vs_payments.csv`

## Resume Bullet

Built a production-style airline Medallion ETL platform using Python, PySpark and SQL to ingest operational, booking, payment, baggage and maintenance data; implemented Bronze/Silver/Gold layers, SCD Type 2 dimensions, data-quality quarantine, revenue reconciliation, and analytics marts for on-time performance, route economics, load factor, fleet reliability and passenger behavior.
