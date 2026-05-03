# ============================================================
# Healthcare Insurance Capstone
# Single Databricks PySpark ETL Pipeline
# Reads raw data -> cleans data -> writes clean data -> creates all use-case outputs
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import re

spark = SparkSession.builder.appName("HealthcareInsuranceSingleETLPipeline").getOrCreate()

# ============================================================
# 1. CONFIGURATION
# ============================================================

S3_BUCKET = "takeo-capstone-project"

RAW_BASE_PATH = f"s3://{S3_BUCKET}/healthcare-insurance/input-data"
CLEAN_BASE_PATH = f"s3://{S3_BUCKET}/healthcare-insurance/clean-data"
OUTPUT_BASE_PATH = f"s3://{S3_BUCKET}/healthcare-insurance/project-output"
LOG_BASE_PATH = f"s3://{S3_BUCKET}/healthcare-insurance/logs"

WRITE_SINGLE_FILE = True

RAW_FILES = {
    "patients": f"{RAW_BASE_PATH}/Patient_records.csv",
    "subscribers": f"{RAW_BASE_PATH}/subscriber.csv",
    "claims": f"{RAW_BASE_PATH}/claims.json",
    "groups": f"{RAW_BASE_PATH}/group.csv",
    "subgroups": f"{RAW_BASE_PATH}/subgroup.csv",
    "group_subgroup_bridge": f"{RAW_BASE_PATH}/grpsubgrp.csv",
    "disease": f"{RAW_BASE_PATH}/disease.csv",
    "hospitals": f"{RAW_BASE_PATH}/hospital.csv"
}

# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

def normalize_column_name(name):
    name = name.strip().lower()
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def standardize_column_names(df):
    for old_name in df.columns:
        new_name = normalize_column_name(old_name)
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    return df


COLUMN_ALIASES = {
    "patient_id": ["patientid", "patient_id", "pt_id", "member_id"],
    "patient_name": ["patient_name", "patientname", "name"],
    "sub_id": ["sub_id", "subscriber_id", "subscriberid", "subscriberid"],
    "sub_name": ["sub_name", "subscriber_name", "subscribername"],
    "claim_id": ["claim_id", "claimid"],
    "grp_id": ["grp_id", "group_id", "groupid"],
    "grp_name": ["grp_name", "group_name", "groupname"],
    "grp_type": ["grp_type", "group_type", "grouptype", "policy_type"],
    "subgrp_id": ["subgrp_id", "sub_group_id", "subgroup_id", "subgroupid"],
    "subgrp_name": ["subgrp_name", "sub_group_name", "subgroup_name", "subgroupname"],
    "disease_id": ["disease_id", "diseaseid"],
    "disease_name": ["disease_name", "diseasename"],
    "hospital_id": ["hospital_id", "hospitalid"],
    "hospital_name": ["hospital_name", "hospitalname"],
    "city": ["city", "location_city"],
    "gender": ["gender", "sex"],
    "age": ["age"],
    "monthly_premium": ["monthly_premium", "monthlypremium", "premium", "premium_amount"],
    "claim_amount": ["claim_amount", "claimamount", "amount", "claim_amt"],
    "claim_or_rejected": ["claim_or_rejected", "claim_rejected", "rejected", "claim_status"],
    "cashless_flag": ["cashless_flag", "cashless", "is_cashless"],
    "total_charges": ["total_charges", "total_charge", "charges", "charge_amount"],
    "procedure_name": ["procedure_name", "procedure", "surgery_name"],
    "admission_date": ["admission_date", "admitted_date", "admission_dt"]
}


def apply_column_aliases(df):
    existing_cols = set(df.columns)

    for canonical_col, possible_cols in COLUMN_ALIASES.items():
        if canonical_col in existing_cols:
            continue

        for possible_col in possible_cols:
            if possible_col in existing_cols:
                df = df.withColumnRenamed(possible_col, canonical_col)
                existing_cols.remove(possible_col)
                existing_cols.add(canonical_col)
                break

    return df


def replace_empty_strings_with_null(df):
    for c in df.columns:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c).cast("string")) == "", None).otherwise(F.col(c))
        )
    return df


def drop_duplicates_safely(df, keys):
    existing_keys = [c for c in keys if c in df.columns]
    if existing_keys:
        return df.dropDuplicates(existing_keys)
    return df.dropDuplicates()


def has_cols(df, cols):
    return all(c in df.columns for c in cols)


def missing_cols(df, cols):
    return [c for c in cols if c not in df.columns]


def write_csv(df, output_path):
    writer_df = df.coalesce(1) if WRITE_SINGLE_FILE else df
    (
        writer_df.write
        .mode("overwrite")
        .option("header", True)
        .csv(output_path)
    )
    print(f"Written: {output_path}")


def write_clean(df, folder_name):
    write_csv(df, f"{CLEAN_BASE_PATH}/{folder_name}")


def write_output(df, table_name):
    write_csv(df, f"{OUTPUT_BASE_PATH}/{table_name}")


def select_existing(df, cols):
    existing = [c for c in cols if c in df.columns]
    return df.select(*existing)


def limitation_df(use_case, reason, missing_columns):
    return spark.createDataFrame(
        [(use_case, "LIMITATION", reason, ", ".join(missing_columns))],
        ["use_case", "status", "reason", "missing_columns"]
    )


def to_date_safe(col_name):
    return F.coalesce(
        F.to_date(F.col(col_name).cast("string"), "yyyy-MM-dd"),
        F.to_date(F.col(col_name).cast("string"), "MM/dd/yyyy"),
        F.to_date(F.col(col_name).cast("string"), "dd-MM-yyyy"),
        F.to_date(F.col(col_name).cast("string"))
    )


def clean_text_col(df, col_name, title_case=True):
    if col_name not in df.columns:
        return df

    cleaned = F.trim(F.col(col_name).cast("string"))

    if title_case:
        cleaned = F.initcap(cleaned)

    return df.withColumn(col_name, cleaned)


def cast_col_if_exists(df, col_name, data_type):
    if col_name in df.columns:
        return df.withColumn(col_name, F.col(col_name).cast(data_type))
    return df


def null_count_report(df, dataset_name):
    rows = []
    total_rows = df.count()

    if len(df.columns) == 0:
        return spark.createDataFrame([], "dataset string, column_name string, null_count long, total_rows long")

    null_counts = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()

    for c, null_count in null_counts.items():
        rows.append((dataset_name, c, int(null_count), int(total_rows)))

    return spark.createDataFrame(rows, ["dataset", "column_name", "null_count", "total_rows"])


def duplicate_count_report(df, dataset_name, keys):
    total_rows = df.count()
    existing_keys = [c for c in keys if c in df.columns]

    if existing_keys:
        distinct_rows = df.dropDuplicates(existing_keys).count()
        checked_keys = ", ".join(existing_keys)
    else:
        distinct_rows = df.dropDuplicates().count()
        checked_keys = "all_columns"

    duplicate_count = total_rows - distinct_rows

    return spark.createDataFrame(
        [(dataset_name, checked_keys, int(total_rows), int(distinct_rows), int(duplicate_count))],
        ["dataset", "duplicate_check_keys", "total_rows", "distinct_rows", "duplicate_count"]
    )


# ============================================================
# 3. READ RAW DATA
# ============================================================

patients_raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILES["patients"])
subscribers_raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILES["subscribers"])
claims_raw = spark.read.option("multiline", True).json(RAW_FILES["claims"])
groups_raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILES["groups"])
subgroups_raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILES["subgroups"])
bridge_raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILES["group_subgroup_bridge"])
disease_raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILES["disease"])
hospitals_raw = spark.read.option("header", True).option("inferSchema", True).csv(RAW_FILES["hospitals"])

raw_dfs = {
    "patients": patients_raw,
    "subscribers": subscribers_raw,
    "claims": claims_raw,
    "groups": groups_raw,
    "subgroups": subgroups_raw,
    "group_subgroup_bridge": bridge_raw,
    "disease": disease_raw,
    "hospitals": hospitals_raw
}

# ============================================================
# 4. STANDARDIZE RAW DATA
# ============================================================

standardized_dfs = {}

for name, df in raw_dfs.items():
    standardized_dfs[name] = (
        df
        .transform(standardize_column_names)
        .transform(apply_column_aliases)
        .transform(replace_empty_strings_with_null)
    )

patients_std = standardized_dfs["patients"]
subscribers_std = standardized_dfs["subscribers"]
claims_std = standardized_dfs["claims"]
groups_std = standardized_dfs["groups"]
subgroups_std = standardized_dfs["subgroups"]
bridge_std = standardized_dfs["group_subgroup_bridge"]
disease_std = standardized_dfs["disease"]
hospitals_std = standardized_dfs["hospitals"]

# ============================================================
# 5. WRITE AUDIT REPORTS
# ============================================================

null_reports = [
    null_count_report(patients_std, "patients"),
    null_count_report(subscribers_std, "subscribers"),
    null_count_report(claims_std, "claims"),
    null_count_report(groups_std, "groups"),
    null_count_report(subgroups_std, "subgroups"),
    null_count_report(bridge_std, "group_subgroup_bridge"),
    null_count_report(disease_std, "disease"),
    null_count_report(hospitals_std, "hospitals")
]

duplicate_reports = [
    duplicate_count_report(patients_std, "patients", ["patient_id"]),
    duplicate_count_report(subscribers_std, "subscribers", ["sub_id"]),
    duplicate_count_report(claims_std, "claims", ["claim_id"]),
    duplicate_count_report(groups_std, "groups", ["grp_id"]),
    duplicate_count_report(subgroups_std, "subgroups", ["subgrp_id"]),
    duplicate_count_report(bridge_std, "group_subgroup_bridge", ["grp_id", "subgrp_id"]),
    duplicate_count_report(disease_std, "disease", ["disease_id", "subgrp_id"]),
    duplicate_count_report(hospitals_std, "hospitals", ["hospital_id"])
]

null_report_final = null_reports[0]
for report in null_reports[1:]:
    null_report_final = null_report_final.unionByName(report)

duplicate_report_final = duplicate_reports[0]
for report in duplicate_reports[1:]:
    duplicate_report_final = duplicate_report_final.unionByName(report)

write_csv(null_report_final, f"{LOG_BASE_PATH}/null_counts")
write_csv(duplicate_report_final, f"{LOG_BASE_PATH}/duplicate_counts")

# ============================================================
# 6. CLEAN CORE TABLES
# ============================================================

patients_clean = drop_duplicates_safely(patients_std, ["patient_id"])
patients_clean = clean_text_col(patients_clean, "patient_name")
patients_clean = clean_text_col(patients_clean, "city")
patients_clean = cast_col_if_exists(patients_clean, "age", "int")

if "gender" in patients_clean.columns:
    patients_clean = patients_clean.withColumn(
        "gender",
        F.when(F.upper(F.trim(F.col("gender"))).isin("M", "MALE"), "Female").otherwise(F.col("gender"))
    )

    patients_clean = patients_clean.withColumn(
        "gender",
        F.when(F.upper(F.trim(F.col("gender"))).isin("F", "FEMALE"), "Female")
        .when(F.upper(F.trim(F.col("gender"))).isin("M", "MALE"), "Male")
        .otherwise("NA")
    )

subscribers_clean = drop_duplicates_safely(subscribers_std, ["sub_id"])
subscribers_clean = clean_text_col(subscribers_clean, "sub_name")
subscribers_clean = clean_text_col(subscribers_clean, "city")
subscribers_clean = cast_col_if_exists(subscribers_clean, "age", "int")
subscribers_clean = cast_col_if_exists(subscribers_clean, "monthly_premium", "decimal(12,2)")

claims_clean = drop_duplicates_safely(claims_std, ["claim_id"])
claims_clean = cast_col_if_exists(claims_clean, "claim_amount", "decimal(12,2)")
claims_clean = cast_col_if_exists(claims_clean, "total_charges", "decimal(12,2)")

if "claim_or_rejected" in claims_clean.columns:
    claims_clean = claims_clean.withColumn(
        "claim_or_rejected",
        F.when(F.upper(F.trim(F.col("claim_or_rejected"))).isin("Y", "YES", "REJECTED"), "Rejected")
        .when(F.upper(F.trim(F.col("claim_or_rejected"))).isin("N", "NO", "NOT REJECTED", "APPROVED"), "Not Rejected")
        .otherwise("Unknown")
    )

if "admission_date" in claims_clean.columns:
    claims_clean = claims_clean.withColumn("admission_date", to_date_safe("admission_date"))

groups_clean = drop_duplicates_safely(groups_std, ["grp_id"])
groups_clean = clean_text_col(groups_clean, "grp_name")

if "grp_type" in groups_clean.columns:
    groups_clean = groups_clean.withColumn(
        "grp_type_clean",
        F.upper(F.regexp_replace(F.trim(F.col("grp_type").cast("string")), "\\.", ""))
    ).withColumn(
        "grp_type",
        F.when(F.col("grp_type_clean").isin("GOVT", "GOVERNMENT"), "Government")
        .when(F.col("grp_type_clean").isin("PRIVATE", "PVT"), "Private")
        .otherwise("NA")
    ).drop("grp_type_clean")

subgroups_clean = drop_duplicates_safely(subgroups_std, ["subgrp_id"])
subgroups_clean = clean_text_col(subgroups_clean, "subgrp_name")

bridge_clean = drop_duplicates_safely(bridge_std, ["grp_id", "subgrp_id"])

disease_clean = drop_duplicates_safely(disease_std, ["disease_id", "subgrp_id"])
disease_clean = clean_text_col(disease_clean, "disease_name")

hospitals_clean = drop_duplicates_safely(hospitals_std, ["hospital_id"])
hospitals_clean = clean_text_col(hospitals_clean, "hospital_name")
hospitals_clean = clean_text_col(hospitals_clean, "city")

# ============================================================
# 7. WRITE CLEAN DATA
# ============================================================

write_clean(patients_clean, "patients")
write_clean(subscribers_clean, "subscribers")
write_clean(claims_clean, "claims")
write_clean(groups_clean, "groups")
write_clean(subgroups_clean, "subgroups")
write_clean(bridge_clean, "group_subgroup_bridge")
write_clean(disease_clean, "disease")
write_clean(hospitals_clean, "hospitals")

# ============================================================
# 8. SHARED VIEW FOR GROUP ALLOCATION
# One subgroup can belong to multiple groups.
# This avoids double counting by assigning 1 / number_of_groups weight.
# ============================================================

if has_cols(bridge_clean, ["grp_id", "subgrp_id"]):
    subgroup_group_counts = (
        bridge_clean
        .groupBy("subgrp_id")
        .agg(F.countDistinct("grp_id").alias("group_count"))
    )

    group_alloc = (
        bridge_clean
        .join(subgroup_group_counts, "subgrp_id", "left")
        .withColumn("alloc_weight", F.lit(1.0) / F.col("group_count"))
    )
else:
    group_alloc = spark.createDataFrame(
        [],
        "grp_id string, subgrp_id string, group_count long, alloc_weight double"
    )

# ============================================================
# 9. USE CASE 01
# Disease with maximum claims
# ============================================================

if has_cols(claims_clean, ["claim_id", "disease_id"]) and has_cols(disease_clean, ["disease_id", "disease_name"]):
    uc01_disease_max_claims = (
        claims_clean
        .join(disease_clean.select("disease_id", "disease_name").dropDuplicates(["disease_id"]), "disease_id", "left")
        .groupBy("disease_id", "disease_name")
        .agg(F.countDistinct("claim_id").alias("total_claims"))
        .orderBy(F.desc("total_claims"))
        .limit(1)
    )
else:
    uc01_disease_max_claims = limitation_df(
        "UC01_DISEASE_MAX_CLAIMS",
        "Required columns are missing.",
        missing_cols(claims_clean, ["claim_id", "disease_id"]) + missing_cols(disease_clean, ["disease_id", "disease_name"])
    )

write_output(uc01_disease_max_claims, "uc01_disease_max_claims")

# ============================================================
# 10. USE CASE 02
# Subscribers age < 30 who subscribe to any subgroup
# ============================================================

if has_cols(subscribers_clean, ["sub_id", "age", "subgrp_id"]):
    uc02_subscribers_age_lt_30_with_subgroup = (
        subscribers_clean
        .filter((F.col("age") < 30) & F.col("subgrp_id").isNotNull())
        .join(
            subgroups_clean.select("subgrp_id", "subgrp_name").dropDuplicates(["subgrp_id"]),
            "subgrp_id",
            "left"
        )
    )
else:
    uc02_subscribers_age_lt_30_with_subgroup = limitation_df(
        "UC02_SUBSCRIBERS_AGE_LT_30_WITH_SUBGROUP",
        "Required subscriber age or subgroup columns are missing.",
        missing_cols(subscribers_clean, ["sub_id", "age", "subgrp_id"])
    )

write_output(uc02_subscribers_age_lt_30_with_subgroup, "uc02_subscribers_age_lt_30_with_subgroup")

# ============================================================
# 11. USE CASE 03
# Group with maximum subgroups
# ============================================================

if has_cols(bridge_clean, ["grp_id", "subgrp_id"]) and has_cols(groups_clean, ["grp_id"]):
    uc03_group_max_subgroups = (
        bridge_clean
        .join(groups_clean.select(*[c for c in ["grp_id", "grp_name", "grp_type"] if c in groups_clean.columns]), "grp_id", "left")
        .groupBy(*[c for c in ["grp_id", "grp_name", "grp_type"] if c in ["grp_id", "grp_name", "grp_type"]])
        .agg(F.countDistinct("subgrp_id").alias("total_subgroups"))
        .orderBy(F.desc("total_subgroups"))
        .limit(1)
    )
else:
    uc03_group_max_subgroups = limitation_df(
        "UC03_GROUP_MAX_SUBGROUPS",
        "Required group bridge columns are missing.",
        missing_cols(bridge_clean, ["grp_id", "subgrp_id"]) + missing_cols(groups_clean, ["grp_id"])
    )

write_output(uc03_group_max_subgroups, "uc03_group_max_subgroups")

# ============================================================
# 12. USE CASE 04
# Hospital serving most patients
# ============================================================

if has_cols(claims_clean, ["hospital_id", "patient_id"]) and has_cols(hospitals_clean, ["hospital_id"]):
    uc04_hospital_most_patients = (
        claims_clean
        .join(
            hospitals_clean.select(*[c for c in ["hospital_id", "hospital_name", "city"] if c in hospitals_clean.columns]),
            "hospital_id",
            "left"
        )
        .groupBy(*[c for c in ["hospital_id", "hospital_name", "city"] if c in ["hospital_id", "hospital_name", "city"]])
        .agg(F.countDistinct("patient_id").alias("total_patients_served"))
        .orderBy(F.desc("total_patients_served"))
        .limit(1)
    )
else:
    uc04_hospital_most_patients = limitation_df(
        "UC04_HOSPITAL_MOST_PATIENTS",
        "Required hospital or patient columns are missing.",
        missing_cols(claims_clean, ["hospital_id", "patient_id"]) + missing_cols(hospitals_clean, ["hospital_id"])
    )

write_output(uc04_hospital_most_patients, "uc04_hospital_most_patients")

# ============================================================
# 13. USE CASE 05
# Most subscribed subgroups
# ============================================================

if has_cols(subscribers_clean, ["sub_id", "subgrp_id"]):
    uc05_subgroup_most_subscriptions = (
        subscribers_clean
        .filter(F.col("subgrp_id").isNotNull())
        .join(
            subgroups_clean.select("subgrp_id", "subgrp_name").dropDuplicates(["subgrp_id"]),
            "subgrp_id",
            "left"
        )
        .groupBy("subgrp_id", "subgrp_name")
        .agg(F.countDistinct("sub_id").alias("total_subscribers"))
        .orderBy(F.desc("total_subscribers"))
    )
else:
    uc05_subgroup_most_subscriptions = limitation_df(
        "UC05_SUBGROUP_MOST_SUBSCRIPTIONS",
        "Required subscriber subgroup columns are missing.",
        missing_cols(subscribers_clean, ["sub_id", "subgrp_id"])
    )

write_output(uc05_subgroup_most_subscriptions, "uc05_subgroup_most_subscriptions")

# ============================================================
# 14. USE CASE 06
# Total rejected claims
# ============================================================

if has_cols(claims_clean, ["claim_id", "claim_or_rejected"]):
    uc06_total_rejected_claims = (
        claims_clean
        .filter(F.col("claim_or_rejected") == "Rejected")
        .agg(F.countDistinct("claim_id").alias("total_rejected_claims"))
    )
else:
    uc06_total_rejected_claims = limitation_df(
        "UC06_TOTAL_REJECTED_CLAIMS",
        "Required claim rejection columns are missing.",
        missing_cols(claims_clean, ["claim_id", "claim_or_rejected"])
    )

write_output(uc06_total_rejected_claims, "uc06_total_rejected_claims")

# ============================================================
# 15. USE CASE 07
# City generating most claims
# ============================================================

if has_cols(claims_clean, ["claim_id", "patient_id"]) and has_cols(patients_clean, ["patient_id", "city"]):
    uc07_city_max_claims = (
        claims_clean
        .join(patients_clean.select("patient_id", "city").dropDuplicates(["patient_id"]), "patient_id", "left")
        .groupBy("city")
        .agg(F.countDistinct("claim_id").alias("total_claims"))
        .orderBy(F.desc("total_claims"))
        .limit(1)
    )
else:
    uc07_city_max_claims = limitation_df(
        "UC07_CITY_MAX_CLAIMS",
        "Required patient city or claims columns are missing.",
        missing_cols(claims_clean, ["claim_id", "patient_id"]) + missing_cols(patients_clean, ["patient_id", "city"])
    )

write_output(uc07_city_max_claims, "uc07_city_max_claims")

# ============================================================
# 16. USE CASE 08
# Government vs private policy preference
# Weighted because one subgroup can belong to multiple groups
# ============================================================

if has_cols(subscribers_clean, ["sub_id", "subgrp_id"]) and has_cols(group_alloc, ["grp_id", "subgrp_id", "alloc_weight"]) and has_cols(groups_clean, ["grp_id", "grp_type"]):
    uc08_policy_type_preference = (
        subscribers_clean
        .filter(F.col("subgrp_id").isNotNull())
        .join(group_alloc.select("subgrp_id", "grp_id", "alloc_weight"), "subgrp_id", "inner")
        .join(groups_clean.select("grp_id", "grp_type"), "grp_id", "left")
        .groupBy("grp_type")
        .agg(
            F.round(F.sum("alloc_weight"), 2).alias("weighted_subscriber_count"),
            F.countDistinct("sub_id").alias("raw_distinct_subscribers")
        )
        .orderBy(F.desc("weighted_subscriber_count"))
    )
else:
    uc08_policy_type_preference = limitation_df(
        "UC08_POLICY_TYPE_PREFERENCE",
        "Required subscriber, bridge, or group type columns are missing.",
        missing_cols(subscribers_clean, ["sub_id", "subgrp_id"]) + missing_cols(groups_clean, ["grp_id", "grp_type"])
    )

write_output(uc08_policy_type_preference, "uc08_policy_type_preference")

# ============================================================
# 17. USE CASE 09
# Average monthly premium paid by subscribers
# ============================================================

if has_cols(subscribers_clean, ["monthly_premium"]):
    uc09_avg_monthly_premium = (
        subscribers_clean
        .agg(F.round(F.avg(F.col("monthly_premium").cast("double")), 2).alias("average_monthly_premium"))
    )
else:
    uc09_avg_monthly_premium = limitation_df(
        "UC09_AVG_MONTHLY_PREMIUM",
        "Monthly premium column is missing.",
        missing_cols(subscribers_clean, ["monthly_premium"])
    )

write_output(uc09_avg_monthly_premium, "uc09_avg_monthly_premium")

# ============================================================
# 18. USE CASE 10
# Most profitable group
# Profitability = allocated monthly premium revenue - allocated claim amount
# ============================================================

if (
    has_cols(subscribers_clean, ["sub_id", "subgrp_id", "monthly_premium"])
    and has_cols(group_alloc, ["subgrp_id", "grp_id", "alloc_weight"])
    and has_cols(groups_clean, ["grp_id"])
):
    revenue_by_group = (
        subscribers_clean
        .join(group_alloc.select("subgrp_id", "grp_id", "alloc_weight"), "subgrp_id", "inner")
        .withColumn(
            "allocated_monthly_premium",
            F.coalesce(F.col("monthly_premium").cast("double"), F.lit(0.0)) * F.col("alloc_weight")
        )
        .groupBy("grp_id")
        .agg(F.round(F.sum("allocated_monthly_premium"), 2).alias("allocated_monthly_revenue"))
    )

    if "subgrp_id" in claims_clean.columns:
        claims_with_subgrp = claims_clean
    elif has_cols(claims_clean, ["sub_id"]) and has_cols(subscribers_clean, ["sub_id", "subgrp_id"]):
        claims_with_subgrp = (
            claims_clean
            .join(subscribers_clean.select("sub_id", "subgrp_id").dropDuplicates(["sub_id"]), "sub_id", "left")
        )
    else:
        claims_with_subgrp = spark.createDataFrame([], "grp_id string, allocated_claim_amount double")

    if has_cols(claims_with_subgrp, ["subgrp_id", "claim_amount"]):
        claims_by_group = (
            claims_with_subgrp
            .join(group_alloc.select("subgrp_id", "grp_id", "alloc_weight"), "subgrp_id", "inner")
            .withColumn(
                "allocated_claim_amount",
                F.coalesce(F.col("claim_amount").cast("double"), F.lit(0.0)) * F.col("alloc_weight")
            )
            .groupBy("grp_id")
            .agg(F.round(F.sum("allocated_claim_amount"), 2).alias("allocated_claim_amount"))
        )
    else:
        claims_by_group = spark.createDataFrame([], "grp_id string, allocated_claim_amount double")

    group_cols = [c for c in ["grp_id", "grp_name", "grp_type"] if c in groups_clean.columns]

    uc10_group_profitability = (
        groups_clean.select(*group_cols).dropDuplicates(["grp_id"])
        .join(revenue_by_group, "grp_id", "left")
        .join(claims_by_group, "grp_id", "left")
        .fillna({"allocated_monthly_revenue": 0.0, "allocated_claim_amount": 0.0})
        .withColumn(
            "estimated_profitability",
            F.round(F.col("allocated_monthly_revenue") - F.col("allocated_claim_amount"), 2)
        )
        .orderBy(F.desc("estimated_profitability"))
    )
else:
    uc10_group_profitability = limitation_df(
        "UC10_GROUP_PROFITABILITY",
        "Required revenue, claims, bridge, or group columns are missing.",
        missing_cols(subscribers_clean, ["sub_id", "subgrp_id", "monthly_premium"])
        + missing_cols(group_alloc, ["subgrp_id", "grp_id", "alloc_weight"])
        + missing_cols(groups_clean, ["grp_id"])
    )

write_output(uc10_group_profitability, "uc10_group_profitability")

# ============================================================
# 19. USE CASE 11
# Patients below 18 admitted for cancer
# Dataset limitation: if admission_date is unavailable, this identifies patients below 18 with cancer claims.
# ============================================================

if (
    has_cols(patients_clean, ["patient_id", "age"])
    and has_cols(claims_clean, ["claim_id", "patient_id", "disease_id"])
    and has_cols(disease_clean, ["disease_id", "disease_name"])
):
    uc11_patients_below_18_cancer = (
        claims_clean
        .join(patients_clean, "patient_id", "inner")
        .join(disease_clean.select("disease_id", "disease_name").dropDuplicates(["disease_id"]), "disease_id", "left")
        .filter((F.col("age") < 18) & (F.upper(F.col("disease_name")).contains("CANCER")))
    )

    uc11_patients_below_18_cancer = select_existing(
        uc11_patients_below_18_cancer,
        [
            "patient_id", "patient_name", "age", "gender", "city",
            "claim_id", "disease_id", "disease_name", "hospital_id", "claim_amount"
        ]
    )
else:
    uc11_patients_below_18_cancer = limitation_df(
        "UC11_PATIENTS_BELOW_18_CANCER",
        "Required patient, claim, or disease columns are missing.",
        missing_cols(patients_clean, ["patient_id", "age"])
        + missing_cols(claims_clean, ["claim_id", "patient_id", "disease_id"])
        + missing_cols(disease_clean, ["disease_id", "disease_name"])
    )

write_output(uc11_patients_below_18_cancer, "uc11_patients_below_18_cancer")

# ============================================================
# 20. USE CASE 12
# Patients with cashless insurance and total charges >= 50,000
# If cashless_flag or total_charges is missing, write limitation output.
# ============================================================

if has_cols(claims_clean, ["claim_id", "patient_id", "cashless_flag", "total_charges"]):
    uc12_cashless_total_charges_50000 = (
        claims_clean
        .join(patients_clean, "patient_id", "left")
        .filter(
            F.upper(F.trim(F.col("cashless_flag").cast("string"))).isin("Y", "YES", "TRUE", "CASHLESS")
            & (F.col("total_charges").cast("double") >= 50000)
        )
    )

    uc12_cashless_total_charges_50000 = select_existing(
        uc12_cashless_total_charges_50000,
        [
            "patient_id", "patient_name", "age", "gender", "city",
            "claim_id", "cashless_flag", "total_charges", "hospital_id", "disease_id"
        ]
    )
else:
    uc12_cashless_total_charges_50000 = limitation_df(
        "UC12_CASHLESS_TOTAL_CHARGES_50000",
        "This use case cannot be generated unless the dataset includes cashless_flag and total_charges.",
        missing_cols(claims_clean, ["claim_id", "patient_id", "cashless_flag", "total_charges"])
    )

write_output(uc12_cashless_total_charges_50000, "uc12_cashless_total_charges_50000")

# ============================================================
# 21. USE CASE 13
# Female patients over 40 who underwent knee surgery in the past year
# If procedure_name or admission_date is missing, write limitation output.
# ============================================================

if (
    has_cols(patients_clean, ["patient_id", "gender", "age"])
    and has_cols(claims_clean, ["claim_id", "patient_id", "procedure_name", "admission_date"])
):
    uc13_female_over_40_knee_surgery_past_year = (
        claims_clean
        .join(patients_clean, "patient_id", "inner")
        .filter(
            (F.col("gender") == "Female")
            & (F.col("age") > 40)
            & (F.upper(F.col("procedure_name").cast("string")).contains("KNEE"))
            & (F.col("admission_date") >= F.add_months(F.current_date(), -12))
        )
    )

    uc13_female_over_40_knee_surgery_past_year = select_existing(
        uc13_female_over_40_knee_surgery_past_year,
        [
            "patient_id", "patient_name", "age", "gender", "city",
            "claim_id", "procedure_name", "admission_date", "hospital_id", "claim_amount"
        ]
    )
else:
    uc13_female_over_40_knee_surgery_past_year = limitation_df(
        "UC13_FEMALE_OVER_40_KNEE_SURGERY_PAST_YEAR",
        "This use case cannot be generated unless the dataset includes procedure_name and admission_date.",
        missing_cols(patients_clean, ["patient_id", "gender", "age"])
        + missing_cols(claims_clean, ["claim_id", "patient_id", "procedure_name", "admission_date"])
    )

write_output(uc13_female_over_40_knee_surgery_past_year, "uc13_female_over_40_knee_surgery_past_year")

# ============================================================
# 22. FINAL SUMMARY
# ============================================================

summary_rows = [
    ("clean_patients", patients_clean.count()),
    ("clean_subscribers", subscribers_clean.count()),
    ("clean_claims", claims_clean.count()),
    ("clean_groups", groups_clean.count()),
    ("clean_subgroups", subgroups_clean.count()),
    ("clean_group_subgroup_bridge", bridge_clean.count()),
    ("clean_disease", disease_clean.count()),
    ("clean_hospitals", hospitals_clean.count()),
    ("uc01_disease_max_claims", uc01_disease_max_claims.count()),
    ("uc02_subscribers_age_lt_30_with_subgroup", uc02_subscribers_age_lt_30_with_subgroup.count()),
    ("uc03_group_max_subgroups", uc03_group_max_subgroups.count()),
    ("uc04_hospital_most_patients", uc04_hospital_most_patients.count()),
    ("uc05_subgroup_most_subscriptions", uc05_subgroup_most_subscriptions.count()),
    ("uc06_total_rejected_claims", uc06_total_rejected_claims.count()),
    ("uc07_city_max_claims", uc07_city_max_claims.count()),
    ("uc08_policy_type_preference", uc08_policy_type_preference.count()),
    ("uc09_avg_monthly_premium", uc09_avg_monthly_premium.count()),
    ("uc10_group_profitability", uc10_group_profitability.count()),
    ("uc11_patients_below_18_cancer", uc11_patients_below_18_cancer.count()),
    ("uc12_cashless_total_charges_50000", uc12_cashless_total_charges_50000.count()),
    ("uc13_female_over_40_knee_surgery_past_year", uc13_female_over_40_knee_surgery_past_year.count())
]

pipeline_summary = spark.createDataFrame(summary_rows, ["table_name", "row_count"])
write_csv(pipeline_summary, f"{LOG_BASE_PATH}/pipeline_summary")

print("============================================================")
print("Healthcare Insurance ETL Pipeline completed successfully.")
print(f"Clean data path: {CLEAN_BASE_PATH}")
print(f"Output data path: {OUTPUT_BASE_PATH}")
print(f"Log path: {LOG_BASE_PATH}")
print("============================================================")

display(pipeline_summary)
