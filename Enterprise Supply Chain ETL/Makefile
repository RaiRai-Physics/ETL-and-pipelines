install:
	pip install -r requirements.txt
profile:
	python src/python_etl/00_profile_sources.py
python-etl:
	python src/python_etl/01_run_enterprise_etl.py
pyspark-etl:
	python src/pyspark_etl/01_run_enterprise_pyspark_etl.py
test:
	pytest
all: profile python-etl
