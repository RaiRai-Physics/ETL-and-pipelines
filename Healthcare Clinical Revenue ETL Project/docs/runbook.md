# Runbook

1. `pip install -r requirements.txt`
2. `python src/python_etl/00_profile_sources.py`
3. Review `reports/python/raw_table_profile.csv` and `raw_column_profile.csv`.
4. `python src/python_etl/01_run_healthcare_etl.py`
5. Review `reports/python/data_quality_report.csv`.
6. Inspect quarantined records under `data/quarantine/python/`.
7. Use trusted analytics under `data/gold/python/`.
8. Check `metadata/run_watermarks.json` before an incremental run.
