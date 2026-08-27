#!/usr/bin/env bash
set -e
python src/python_etl/00_profile_sources.py
python src/python_etl/01_run_healthcare_etl.py
