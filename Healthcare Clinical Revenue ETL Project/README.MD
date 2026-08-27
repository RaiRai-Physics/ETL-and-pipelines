# Healthcare Clinical + Revenue Cycle ETL Project

Healthcare data engineering project. Simulation of a hospital analytics platform combining EHR, appointments, diagnoses, procedures, laboratory, medication administration, claims, payments, bed utilization, patient experience, and CDC feeds.

**All patient/provider identities and records in this project are synthetic.**

## Architecture

```text
Raw source files
      ↓
Bronze landing layer
      ↓
Silver standardized layer
      ├── Data-quality quarantine
      ↓
Gold dimensional model
      ↓
Clinical + Revenue + Operational marts
      ↓
Audit logs + incremental watermarks
```

## Raw Domains

**Master/reference:** facilities, departments, providers, payers, patients, diagnoses, procedures, medications, lab reference, calendar.

**Clinical:** appointments, encounters, diagnoses, procedures, lab orders/results, medication orders/administration.

**Revenue:** claims, claim lines, claim payments.

**Operations:** bed occupancy and patient satisfaction.

**CDC:** patient coverage/status changes and provider specialty/status changes.

## Engineering Features

- Bronze / Silver / Gold architecture
- Source profiling and schema inspection
- Config-driven thresholds
- Data contracts
- PII masking in the clean layer
- Deduplication and PK/FK validation
- Quarantine layer with explicit failure reason
- Patient SCD Type 2
- Provider SCD Type 2
- 30-day readmission calculations
- Length-of-stay analytics
- Disease-burden mart
- Lab turnaround and abnormal-result enrichment
- Medication-administration compliance metrics
- Claim/payment reconciliation
- Denial and payer performance
- Bed occupancy and boarding analytics
- Appointment no-show metrics
- Patient experience analytics
- Provider and department quality marts
- Patient 360
- Audit log and incremental watermarks
- Python and PySpark implementations
- SQL DDL and analytics queries
- Unit tests, Airflow DAG, Dockerfile, Makefile

## Run Python Pipeline

```bash
pip install -r requirements.txt
python src/python_etl/00_profile_sources.py
python src/python_etl/01_run_healthcare_etl.py
```

Or:

```bash
bash run_python_pipeline.sh
```

## Run PySpark Core Pipeline

```bash
python src/pyspark_etl/01_run_healthcare_pyspark_etl.py
```

## Run Tests

```bash
pytest -q
```

## Important Gold Outputs

- `dim_patient_scd2.csv`
- `dim_provider_scd2.csv`
- `fact_encounters.csv`
- `fact_encounter_diagnoses.csv`
- `fact_encounter_procedures.csv`
- `fact_lab_results.csv`
- `fact_medication_administration.csv`
- `fact_claims.csv`
- `fact_claim_lines.csv`
- `fact_bed_occupancy.csv`
- `fact_appointments.csv`
- `fact_patient_satisfaction.csv`
- `mart_readmissions_30d.csv`
- `mart_disease_burden.csv`
- `mart_lab_turnaround.csv`
- `mart_medication_administration.csv`
- `mart_payer_performance.csv`
- `mart_revenue_cycle_summary.csv`
- `mart_bed_utilization.csv`
- `mart_appointment_access.csv`
- `mart_patient_experience.csv`
- `mart_provider_performance.csv`
- `mart_department_quality.csv`
- `mart_patient_360.csv`


Built a production-style healthcare ETL platform using Python, PySpark, and SQL to integrate EHR, lab, medication, claims, payment, scheduling, and hospital operations data; implemented SCD Type 2 dimensions, data-quality quarantine, 30-day readmission analytics, revenue-cycle reconciliation, bed-utilization monitoring, and clinical/operational marts.
