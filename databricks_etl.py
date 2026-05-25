#This Databricks notebook builds a small but realistic healthcare insurance ETL pipeline using synthetic data only.
#**Raw files**: synthetic CSV source data written to DBFS
# **Bronze**: raw ingested Delta tables with ingestion metadata
#**Silver**: cleaned and validated patient, policy, provider, and claim tables
# **Rejects**: invalid claim records with clear rejection reasons
#**Gold**: analytics-ready fact table and insurance metrics

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# 1. Configuration

dbutils.widgets.text("database_name", "healthcare_insurance_demo")
dbutils.widgets.text("base_path", "dbfs:/tmp/healthcare_insurance_databricks_etl")
dbutils.widgets.dropdown("reset_demo", "true", ["true", "false"])

DATABASE_NAME = dbutils.widgets.get("database_name")
BASE_PATH = dbutils.widgets.get("base_path")
RESET_DEMO = dbutils.widgets.get("reset_demo").lower() == "true"

RAW_PATH = f"{BASE_PATH}/raw"
BRONZE_PATH = f"{BASE_PATH}/bronze"
SILVER_PATH = f"{BASE_PATH}/silver"
GOLD_PATH = f"{BASE_PATH}/gold"
REJECT_PATH = f"{BASE_PATH}/rejects"
AUDIT_PATH = f"{BASE_PATH}/audit"

PIPELINE_RUN_ID = str(uuid.uuid4())
PIPELINE_RUN_TS = datetime.utcnow().isoformat()

print(f"Database: {DATABASE_NAME}")
print(f"Base path: {BASE_PATH}")
print(f"Pipeline run id: {PIPELINE_RUN_ID}")

#2. Reset demo environment

# COMMAND ----------

if RESET_DEMO:
    dbutils.fs.rm(BASE_PATH, recurse=True)
    spark.sql(f"DROP DATABASE IF EXISTS {DATABASE_NAME} CASCADE")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")

# 3. Create synthetic raw source data

patients_data = [
    (1, "Asha Patel", 29, "F", "Dallas", "TX"),
    (2, "Ben Carter", 44, "M", "Austin", "TX"),
    (3, "Mina Lee", 17, "F", "Chicago", "IL"),
    (4, "Leo Smith", 63, "M", "Houston", "TX"),
    (5, "Rina Shah", 52, "Female", "Phoenix", "AZ"),
    (6, "Omar Khan", 36, "Male", "Seattle", "WA"),
]

policies_data = [
    (1001, "Private", "Gold", 310.00),
    (1002, "Government", "Silver", 120.00),
    (1003, "Private", "Platinum", 450.00),
    (1004, "Private", "Bronze", 180.00),
]

providers_data = [
    (501, "North Care Hospital", "Dallas", "TX"),
    (502, "Lakeview Clinic", "Austin", "TX"),
    (503, "Metro Health Center", "Chicago", "IL"),
    (504, "Desert Valley Hospital", "Phoenix", "AZ"),
]

claims_data = [
    (9001, 1, 1001, 501, "Diabetes", 850.50, "Approved", "2026-01-01"),
    (9002, 2, 1002, 502, "Hypertension", 430.00, "Rejected", "2026-01-01"),
    (9003, 3, 1002, 503, "Cancer", 5200.00, "Approved", "2026-01-02"),
    (9004, 4, 1003, 501, "Knee Surgery", 7300.00, "Pending", "2026-01-02"),
    (9005, 99, 1001, 501, "Flu", 150.00, "Approved", "2026-01-03"),
    (9006, 2, 9999, 502, "Asthma", 600.00, "Approved", "2026-01-03"),
    (9007, 1, 1001, 999, "Migraine", 200.00, "Approved", "2026-01-03"),
    (9008, 1, 1001, 501, "", -50.00, "Approved", "2026-01-04"),
    (9009, 5, 1004, 504, "Cardiology", 1250.75, "Approved", "2026-01-04"),
    (9010, 6, 1003, 503, "Orthopedic", 2100.00, "Approved", "2026-01-05"),
    (9011, 3, 1002, 503, "Cancer", 4800.00, "Rejected", "2026-01-05"),
    (9003, 3, 1002, 503, "Cancer", 5200.00, "Approved", "2026-01-02"),
]

patients_schema = T.StructType([
    T.StructField("patient_id", T.IntegerType(), True),
    T.StructField("patient_name", T.StringType(), True),
    T.StructField("age", T.IntegerType(), True),
    T.StructField("gender", T.StringType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("state", T.StringType(), True),
])

policies_schema = T.StructType([
    T.StructField("policy_id", T.IntegerType(), True),
    T.StructField("policy_type", T.StringType(), True),
    T.StructField("coverage_level", T.StringType(), True),
    T.StructField("monthly_premium", T.DoubleType(), True),
])

providers_schema = T.StructType([
    T.StructField("provider_id", T.IntegerType(), True),
    T.StructField("provider_name", T.StringType(), True),
    T.StructField("provider_city", T.StringType(), True),
    T.StructField("provider_state", T.StringType(), True),
])

claims_schema = T.StructType([
    T.StructField("claim_id", T.IntegerType(), True),
    T.StructField("patient_id", T.IntegerType(), True),
    T.StructField("policy_id", T.IntegerType(), True),
    T.StructField("provider_id", T.IntegerType(), True),
    T.StructField("diagnosis", T.StringType(), True),
    T.StructField("claim_amount", T.DoubleType(), True),
    T.StructField("claim_status", T.StringType(), True),
    T.StructField("claim_date", T.StringType(), True),
])

raw_sources = {
    "patients": spark.createDataFrame(patients_data, patients_schema),
    "policies": spark.createDataFrame(policies_data, policies_schema),
    "providers": spark.createDataFrame(providers_data, providers_schema),
    "claims": spark.createDataFrame(claims_data, claims_schema),
}

for source_name, source_df in raw_sources.items():
    source_path = f"{RAW_PATH}/{source_name}"
    source_df.coalesce(1).write.mode("overwrite").option("header", True).csv(source_path)
    print(f"Wrote raw source: {source_path}")

# 4. Ingest raw CSV files into Bronze Delta tables

source_configs = [
    {
        "name": "patients",
        "path": f"{RAW_PATH}/patients",
        "schema": patients_schema,
        "bronze_table": "bronze_patients",
    },
    {
        "name": "policies",
        "path": f"{RAW_PATH}/policies",
        "schema": policies_schema,
        "bronze_table": "bronze_policies",
    },
    {
        "name": "providers",
        "path": f"{RAW_PATH}/providers",
        "schema": providers_schema,
        "bronze_table": "bronze_providers",
    },
    {
        "name": "claims",
        "path": f"{RAW_PATH}/claims",
        "schema": claims_schema,
        "bronze_table": "bronze_claims",
    },
]

bronze_audit_rows = []

for config in source_configs:
    bronze_df = (
        spark.read
        .option("header", True)
        .schema(config["schema"])
        .csv(config["path"])
        .withColumn("_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
        .withColumn("_ingested_at_utc", F.current_timestamp())
        .withColumn("_source_system", F.lit("synthetic_csv"))
        .withColumn("_source_path", F.lit(config["path"]))
    )

    table_name = config["bronze_table"]
    table_path = f"{BRONZE_PATH}/{table_name}"

    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)

    row_count = bronze_df.count()
    bronze_audit_rows.append((config["name"], table_name, row_count, PIPELINE_RUN_ID))
    print(f"Ingested {row_count} rows into {table_name}")

# 5. Build Silver dimension tables

silver_patients = (
    spark.table(f"{DATABASE_NAME}.bronze_patients")
    .select(
        F.col("patient_id").cast("int"),
        F.initcap(F.trim(F.col("patient_name"))).alias("patient_name"),
        F.col("age").cast("int"),
        F.when(F.upper(F.trim(F.col("gender"))).isin("F", "FEMALE"), F.lit("Female"))
         .when(F.upper(F.trim(F.col("gender"))).isin("M", "MALE"), F.lit("Male"))
         .otherwise(F.lit("Unknown")).alias("gender"),
        F.initcap(F.trim(F.col("city"))).alias("city"),
        F.upper(F.trim(F.col("state"))).alias("state"),
        F.current_timestamp().alias("_silver_updated_at_utc")
    )
    .dropDuplicates(["patient_id"])
)

silver_policies = (
    spark.table(f"{DATABASE_NAME}.bronze_policies")
    .select(
        F.col("policy_id").cast("int"),
        F.initcap(F.trim(F.col("policy_type"))).alias("policy_type"),
        F.initcap(F.trim(F.col("coverage_level"))).alias("coverage_level"),
        F.col("monthly_premium").cast("double"),
        F.current_timestamp().alias("_silver_updated_at_utc")
    )
    .dropDuplicates(["policy_id"])
)

silver_providers = (
    spark.table(f"{DATABASE_NAME}.bronze_providers")
    .select(
        F.col("provider_id").cast("int"),
        F.initcap(F.trim(F.col("provider_name"))).alias("provider_name"),
        F.initcap(F.trim(F.col("provider_city"))).alias("provider_city"),
        F.upper(F.trim(F.col("provider_state"))).alias("provider_state"),
        F.current_timestamp().alias("_silver_updated_at_utc")
    )
    .dropDuplicates(["provider_id"])
)

silver_tables = {
    "silver_patients": silver_patients,
    "silver_policies": silver_policies,
    "silver_providers": silver_providers,
}

for table_name, df in silver_tables.items():
    table_path = f"{SILVER_PATH}/{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)

    print(f"Created {table_name}: {df.count()} rows")

# 6. Clean and validate claims

claims_window = Window.partitionBy("claim_id")

claims_standardized = (
    spark.table(f"{DATABASE_NAME}.bronze_claims")
    .select(
        F.col("claim_id").cast("int"),
        F.col("patient_id").cast("int"),
        F.col("policy_id").cast("int"),
        F.col("provider_id").cast("int"),
        F.initcap(F.trim(F.coalesce(F.col("diagnosis"), F.lit("")))).alias("diagnosis"),
        F.col("claim_amount").cast("double"),
        F.lower(F.trim(F.coalesce(F.col("claim_status"), F.lit("")))).alias("claim_status"),
        F.to_date(F.col("claim_date")).alias("claim_date"),
        F.current_timestamp().alias("_silver_updated_at_utc")
    )
    .withColumn("claim_id_count", F.count("*").over(claims_window))
)

patient_keys = (
    spark.table(f"{DATABASE_NAME}.silver_patients")
    .select("patient_id")
    .distinct()
    .withColumn("patient_key_exists", F.lit(1))
)

policy_keys = (
    spark.table(f"{DATABASE_NAME}.silver_policies")
    .select("policy_id")
    .distinct()
    .withColumn("policy_key_exists", F.lit(1))
)

provider_keys = (
    spark.table(f"{DATABASE_NAME}.silver_providers")
    .select("provider_id")
    .distinct()
    .withColumn("provider_key_exists", F.lit(1))
)

claims_checked = (
    claims_standardized
    .join(patient_keys, on="patient_id", how="left")
    .join(policy_keys, on="policy_id", how="left")
    .join(provider_keys, on="provider_id", how="left")
)

valid_statuses = ["approved", "rejected", "pending"]

reason_columns = [
    F.when(F.col("claim_id").isNull(), F.lit("missing claim_id")),
    F.when(F.col("patient_id").isNull(), F.lit("missing patient_id")),
    F.when(F.col("policy_id").isNull(), F.lit("missing policy_id")),
    F.when(F.col("provider_id").isNull(), F.lit("missing provider_id")),
    F.when(F.col("diagnosis") == "", F.lit("missing diagnosis")),
    F.when(F.col("claim_amount").isNull(), F.lit("invalid claim_amount")),
    F.when(F.col("claim_amount") <= 0, F.lit("claim_amount must be greater than zero")),
    F.when(~F.col("claim_status").isin(valid_statuses), F.lit("invalid claim_status")),
    F.when(F.col("claim_date").isNull(), F.lit("invalid claim_date")),
    F.when(F.col("claim_id_count") > 1, F.lit("duplicate claim_id")),
    F.when(F.col("patient_key_exists").isNull(), F.lit("patient_id not found")),
    F.when(F.col("policy_key_exists").isNull(), F.lit("policy_id not found")),
    F.when(F.col("provider_key_exists").isNull(), F.lit("provider_id not found")),
]

claims_validated = claims_checked.withColumn(
    "reject_reason",
    F.concat_ws("; ", *reason_columns)
)

silver_claims_clean = (
    claims_validated
    .filter(F.col("reject_reason") == "")
    .select(
        "claim_id",
        "patient_id",
        "policy_id",
        "provider_id",
        "diagnosis",
        "claim_amount",
        "claim_status",
        "claim_date",
        "_silver_updated_at_utc"
    )
)

silver_claims_rejected = (
    claims_validated
    .filter(F.col("reject_reason") != "")
    .select(
        "claim_id",
        "patient_id",
        "policy_id",
        "provider_id",
        "diagnosis",
        "claim_amount",
        "claim_status",
        "claim_date",
        "reject_reason",
        "_silver_updated_at_utc"
    )
)

claims_outputs = {
    "silver_claims_clean": silver_claims_clean,
    "silver_claims_rejected": silver_claims_rejected,
}

for table_name, df in claims_outputs.items():
    if table_name.endswith("rejected"):
        table_path = f"{REJECT_PATH}/{table_name}"
    else:
        table_path = f"{SILVER_PATH}/{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)

    print(f"Created {table_name}: {df.count()} rows")

# 7. Build Gold fact table

fact_claims = (
    spark.table(f"{DATABASE_NAME}.silver_claims_clean").alias("c")
    .join(spark.table(f"{DATABASE_NAME}.silver_patients").alias("p"), on="patient_id", how="inner")
    .join(spark.table(f"{DATABASE_NAME}.silver_policies").alias("pol"), on="policy_id", how="inner")
    .join(spark.table(f"{DATABASE_NAME}.silver_providers").alias("pr"), on="provider_id", how="inner")
    .select(
        F.col("c.claim_id"),
        F.col("c.claim_date"),
        F.date_format(F.col("c.claim_date"), "yyyy-MM").alias("claim_month"),
        F.col("c.claim_status"),
        F.col("c.diagnosis"),
        F.col("c.claim_amount"),
        F.col("p.patient_id"),
        F.col("p.age").alias("patient_age"),
        F.col("p.gender").alias("patient_gender"),
        F.col("p.city").alias("patient_city"),
        F.col("p.state").alias("patient_state"),
        F.col("pol.policy_id"),
        F.col("pol.policy_type"),
        F.col("pol.coverage_level"),
        F.col("pol.monthly_premium"),
        F.col("pr.provider_id"),
        F.col("pr.provider_name"),
        F.col("pr.provider_city"),
        F.col("pr.provider_state"),
        F.round(F.col("c.claim_amount") / F.col("pol.monthly_premium"), 2).alias("claim_to_premium_ratio"),
        F.when(F.col("c.claim_amount") >= 5000, F.lit(True)).otherwise(F.lit(False)).alias("is_high_value_claim"),
        F.current_timestamp().alias("_gold_updated_at_utc")
    )
)

fact_claims_path = f"{GOLD_PATH}/gold_fact_claims"

(
    fact_claims.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(fact_claims_path)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_fact_claims
    USING DELTA
    LOCATION '{fact_claims_path}'
""")

print(f"Created gold_fact_claims: {fact_claims.count()} rows")

# 8. Create Gold insurance analytics metrics

gold_fact_claims = spark.table(f"{DATABASE_NAME}.gold_fact_claims")
silver_claims_rejected = spark.table(f"{DATABASE_NAME}.silver_claims_rejected")

gold_claims_by_status = (
    gold_fact_claims
    .groupBy("claim_status")
    .agg(
        F.count("*").alias("total_claims"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount"),
        F.round(F.avg("claim_amount"), 2).alias("avg_claim_amount")
    )
    .orderBy(F.desc("total_claims"))
)

gold_claims_by_policy_type = (
    gold_fact_claims
    .groupBy("policy_type", "coverage_level")
    .agg(
        F.count("*").alias("total_claims"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount"),
        F.round(F.avg("monthly_premium"), 2).alias("avg_monthly_premium"),
        F.round(F.avg("claim_to_premium_ratio"), 2).alias("avg_claim_to_premium_ratio")
    )
    .orderBy(F.desc("total_claim_amount"))
)

gold_top_diagnoses = (
    gold_fact_claims
    .groupBy("diagnosis")
    .agg(
        F.count("*").alias("total_claims"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount"),
        F.round(F.avg("claim_amount"), 2).alias("avg_claim_amount")
    )
    .orderBy(F.desc("total_claim_amount"))
)

gold_provider_claims = (
    gold_fact_claims
    .groupBy("provider_name", "provider_city", "provider_state")
    .agg(
        F.count("*").alias("total_claims"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount")
    )
    .orderBy(F.desc("total_claim_amount"))
)

gold_city_claims = (
    gold_fact_claims
    .groupBy("patient_city", "patient_state")
    .agg(
        F.count("*").alias("total_claims"),
        F.countDistinct("patient_id").alias("unique_patients"),
        F.round(F.sum("claim_amount"), 2).alias("total_claim_amount")
    )
    .orderBy(F.desc("total_claim_amount"))
)

gold_under_18_cancer_claims = (
    gold_fact_claims
    .filter((F.col("patient_age") < 18) & (F.lower(F.col("diagnosis")).contains("cancer")))
    .select(
        "claim_id",
        "claim_date",
        "patient_id",
        "patient_age",
        "patient_gender",
        "diagnosis",
        "claim_amount",
        "policy_type",
        "provider_name"
    )
)

gold_rejected_claims_summary = (
    silver_claims_rejected
    .groupBy("reject_reason")
    .agg(F.count("*").alias("rejected_claim_count"))
    .orderBy(F.desc("rejected_claim_count"))
)

metric_tables = {
    "gold_claims_by_status": gold_claims_by_status,
    "gold_claims_by_policy_type": gold_claims_by_policy_type,
    "gold_top_diagnoses": gold_top_diagnoses,
    "gold_provider_claims": gold_provider_claims,
    "gold_city_claims": gold_city_claims,
    "gold_under_18_cancer_claims": gold_under_18_cancer_claims,
    "gold_rejected_claims_summary": gold_rejected_claims_summary,
}

for table_name, df in metric_tables.items():
    table_path = f"{GOLD_PATH}/{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.{table_name}
        USING DELTA
        LOCATION '{table_path}'
    """)

    print(f"Created {table_name}: {df.count()} rows")

# 9. Create pipeline audit table

audit_rows = [
    ("bronze_patients", spark.table(f"{DATABASE_NAME}.bronze_patients").count()),
    ("bronze_policies", spark.table(f"{DATABASE_NAME}.bronze_policies").count()),
    ("bronze_providers", spark.table(f"{DATABASE_NAME}.bronze_providers").count()),
    ("bronze_claims", spark.table(f"{DATABASE_NAME}.bronze_claims").count()),
    ("silver_patients", spark.table(f"{DATABASE_NAME}.silver_patients").count()),
    ("silver_policies", spark.table(f"{DATABASE_NAME}.silver_policies").count()),
    ("silver_providers", spark.table(f"{DATABASE_NAME}.silver_providers").count()),
    ("silver_claims_clean", spark.table(f"{DATABASE_NAME}.silver_claims_clean").count()),
    ("silver_claims_rejected", spark.table(f"{DATABASE_NAME}.silver_claims_rejected").count()),
    ("gold_fact_claims", spark.table(f"{DATABASE_NAME}.gold_fact_claims").count()),
]

audit_schema = T.StructType([
    T.StructField("table_name", T.StringType(), False),
    T.StructField("row_count", T.LongType(), False),
])

audit_df = (
    spark.createDataFrame(audit_rows, audit_schema)
    .withColumn("pipeline_name", F.lit("healthcare_insurance_claims_etl"))
    .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    .withColumn("pipeline_run_timestamp_utc", F.lit(PIPELINE_RUN_TS))
    .withColumn("audit_created_at_utc", F.current_timestamp())
    .select(
        "pipeline_name",
        "pipeline_run_id",
        "pipeline_run_timestamp_utc",
        "table_name",
        "row_count",
        "audit_created_at_utc"
    )
)

audit_table_path = f"{AUDIT_PATH}/pipeline_audit"

(
    audit_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(audit_table_path)
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.pipeline_audit
    USING DELTA
    LOCATION '{audit_table_path}'
""")

display(audit_df)

# 10. Preview final outputs

print("Gold fact claims")
display(spark.table(f"{DATABASE_NAME}.gold_fact_claims"))

print("Claims by policy type")
display(spark.table(f"{DATABASE_NAME}.gold_claims_by_policy_type"))

print("Top diagnoses")
display(spark.table(f"{DATABASE_NAME}.gold_top_diagnoses"))

print("Rejected claims summary")
display(spark.table(f"{DATABASE_NAME}.gold_rejected_claims_summary"))

# 11. Useful SQL queries
SELECT * FROM healthcare_insurance_demo.gold_fact_claims;
SELECT * FROM healthcare_insurance_demo.gold_claims_by_policy_type;
SELECT * FROM healthcare_insurance_demo.gold_top_diagnoses;
 SELECT * FROM healthcare_insurance_demo.gold_rejected_claims_summary;
 SELECT * FROM healthcare_insurance_demo.pipeline_audit;
