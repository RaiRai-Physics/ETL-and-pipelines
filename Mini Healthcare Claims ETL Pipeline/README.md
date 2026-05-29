# ETL PySpark mini Project

This is a beginner-friendly ETL Data Engineering project using Python and PySpark.

## Project Goal

Build a small ETL pipeline that reads raw healthcare patient data, cleans it, performs data quality checks, and writes clean output files.

## Current Dataset

The project currently uses one raw file:

- `data/raw/patients.csv`

## ETL Flow

1. Extract raw patient data from CSV
2. Clean missing values
3. Standardize gender values
4. Remove duplicate patient records
5. Write cleaned data
6. Generate a data quality report

## Folder Structure

```text
etl-pyspark-pipeline/
├── data/
│   ├── raw/
│   ├── clean/
│   └── output/
├── src/
│   ├── python_etl/
│   │   └── clean_patients.py
│   └── pyspark_etl/
│       └── clean_patients_spark.py
├── reports/
├── README.md
└── requirements.txt
