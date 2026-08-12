# Runbook

1. Install dependencies with `pip install -r requirements.txt`.
2. Run `python src/python_etl/00_profile_sources.py`.
3. Review `reports/python/raw_table_profile.csv`.
4. Run `python src/python_etl/01_run_retail_etl.py`.
5. Review `reports/python/data_quality_report.csv` and `pipeline_audit_log.csv`.
6. Inspect `data/quarantine/python/` for bad rows.
7. Consume trusted analytics from `data/gold/python/`.
8. Check `metadata/run_watermarks.json` for incremental checkpoints.
