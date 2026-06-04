#!/usr/bin/env bash
set -e
python src/python_etl/00_profile_raw_data.py
python src/python_etl/01_run_financial_python_etl.py
