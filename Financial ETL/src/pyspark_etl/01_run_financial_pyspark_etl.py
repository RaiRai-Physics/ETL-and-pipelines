"""
Financial ETL Pipeline using PySpark.

Run from project root:
    python src/pyspark_etl/01_run_financial_pyspark_etl.py

This PySpark version makes the following transformations:
- standardization
- primary/foreign key validation
- currency conversion
- high-risk transaction flags
- customer and merchant marts
- data quality report
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW = str(PROJECT_ROOT / "data" / "raw")
CLEAN = str(PROJECT_ROOT / "data" / "clean" / "pyspark")
QUARANTINE = str(PROJECT_ROOT / "data" / "quarantine" / "pyspark")
MART = str(PROJECT_ROOT / "data" / "marts" / "pyspark")
REPORT = str(PROJECT_ROOT / "reports" / "pyspark")


def spark_session():
    return (
        SparkSession.builder
        .appName("AdvancedFinancialETL")
        .master("local[*]")
        .getOrCreate()
    )


def read_csv(spark, name):
    return spark.read.option("header", True).option("inferSchema", False).csv(f"{RAW}/{name}.csv")


def write_csv(df, path):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def text_col(c, default="Unknown", case=None):
    v = F.trim(F.coalesce(F.col(c), F.lit("")))
    v = F.when(v == "", F.lit(default)).otherwise(v)
    if case == "upper":
        return F.upper(v)
    if case == "lower":
        return F.lower(v)
    if case == "title":
        return F.initcap(v)
    return v


def num_col(c, default=None):
    v = F.regexp_replace(F.trim(F.coalesce(F.col(c), F.lit(""))), "\\$", "")
    v = F.regexp_replace(v, ",", "")
    n = v.cast("double")
    if default is not None:
        n = F.coalesce(n, F.lit(float(default)))
    return n


def parse_ts(c):
    v = F.trim(F.coalesce(F.col(c), F.lit("")))
    return F.coalesce(
        F.to_timestamp(v, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(v, "MM/dd/yyyy HH:mm"),
        F.to_timestamp(v, "yyyy-MM-dd"),
        F.to_timestamp(v, "MM/dd/yyyy"),
    )


def parse_date(c):
    return F.to_date(parse_ts(c))


def bool_col(c):
    v = F.upper(F.trim(F.coalesce(F.col(c), F.lit(""))))
    return (
        F.when(v.isin("Y", "YES", "TRUE", "1", "ACTIVE"), F.lit(True))
        .when(v.isin("N", "NO", "FALSE", "0", "INACTIVE"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def mask_email(c):
    return F.when(F.col(c).contains("@"), F.concat(F.substring(F.col(c), 1, 2), F.lit("***@"), F.split(F.col(c), "@").getItem(1))).otherwise(F.lit("unknown@example.com"))


def clean_core_tables(spark):
    branches = (
        read_csv(spark, "branches")
        .withColumn("branch_id", text_col("branch_id", "", "upper"))
        .withColumn("branch_name", text_col("branch_name", case="title"))
        .withColumn("city", text_col("city", case="title"))
        .withColumn("state", text_col("state", case="upper"))
        .withColumn("region", text_col("region", case="title"))
        .withColumn("open_date", parse_date("open_date"))
        .dropDuplicates(["branch_id"])
    )

    customers = (
        read_csv(spark, "customers")
        .withColumn("customer_id", text_col("customer_id", "", "upper"))
        .withColumn("full_name", text_col("full_name", case="title"))
        .withColumn("email_raw", text_col("email", "unknown@example.com", "lower"))
        .withColumn("email_masked", mask_email("email_raw"))
        .withColumn("date_of_birth", parse_date("date_of_birth"))
        .withColumn("signup_date", parse_date("signup_date"))
        .withColumn("customer_segment", text_col("customer_segment", "Unknown", "title"))
        .withColumn("kyc_status", text_col("kyc_status", "Unknown", "title"))
        .withColumn("risk_rating", text_col("risk_rating", "Unknown", "title"))
        .withColumn("city", text_col("city", case="title"))
        .withColumn("state", text_col("state", case="upper"))
        .withColumn("country", F.when(text_col("country", case="upper").isin("US", "UNITED STATES"), "USA").otherwise(text_col("country", case="upper")))
        .withColumn("is_active", bool_col("is_active"))
        .drop("email", "phone")
        .dropDuplicates(["customer_id"])
    )

    accounts = (
        read_csv(spark, "accounts")
        .withColumn("account_id", text_col("account_id", "", "upper"))
        .withColumn("customer_id", text_col("customer_id", "", "upper"))
        .withColumn("branch_id", text_col("branch_id", "", "upper"))
        .withColumn("account_type", text_col("account_type", case="title"))
        .withColumn("currency", text_col("currency", "USD", "upper"))
        .withColumn("open_date", parse_date("open_date"))
        .withColumn("account_status", text_col("account_status", case="title"))
        .withColumn("opening_balance", num_col("opening_balance", 0))
        .withColumn("overdraft_limit", num_col("overdraft_limit", 0))
        .dropDuplicates(["account_id"])
    )

    merchants = (
        read_csv(spark, "merchants")
        .withColumn("merchant_id", text_col("merchant_id", "", "upper"))
        .withColumn("merchant_category", text_col("merchant_category", case="title"))
        .withColumn("category_group", text_col("category_group", case="title"))
        .withColumn("high_risk_flag", bool_col("high_risk_flag"))
        .dropDuplicates(["merchant_id"])
    )

    cards = (
        read_csv(spark, "cards")
        .withColumn("card_id", text_col("card_id", "", "upper"))
        .withColumn("account_id", text_col("account_id", "", "upper"))
        .withColumn("card_type", text_col("card_type", case="title"))
        .withColumn("card_status", text_col("card_status", case="title"))
        .withColumn("credit_limit", num_col("credit_limit", 0))
        .dropDuplicates(["card_id"])
    )

    rates = (
        read_csv(spark, "exchange_rates")
        .withColumn("rate_date", parse_date("rate_date"))
        .withColumn("currency", text_col("currency", "USD", "upper"))
        .withColumn("usd_rate", num_col("usd_rate"))
        .dropDuplicates(["rate_date", "currency"])
    )

    txns = (
        read_csv(spark, "transactions")
        .withColumn("transaction_id", text_col("transaction_id", "", "upper"))
        .withColumn("account_id", text_col("account_id", "", "upper"))
        .withColumn("card_id", text_col("card_id", "", "upper"))
        .withColumn("merchant_id", text_col("merchant_id", "", "upper"))
        .withColumn("transaction_ts", parse_ts("transaction_ts"))
        .withColumn("transaction_date", F.to_date("transaction_ts"))
        .withColumn("transaction_type", text_col("transaction_type", case="upper"))
        .withColumn("channel", text_col("channel", case="title"))
        .withColumn("direction", text_col("direction", case="lower"))
        .withColumn("amount", num_col("amount"))
        .withColumn("currency", text_col("currency", "USD", "upper"))
        .withColumn("transaction_status", text_col("transaction_status", case="title"))
        .dropDuplicates(["transaction_id"])
    )

    fraud = (
        read_csv(spark, "fraud_alerts")
        .withColumn("alert_id", text_col("alert_id", "", "upper"))
        .withColumn("transaction_id", text_col("transaction_id", "", "upper"))
        .withColumn("alert_severity", text_col("alert_severity", case="title"))
        .dropDuplicates(["alert_id"])
    )

    loans = (
        read_csv(spark, "loans")
        .withColumn("loan_id", text_col("loan_id", "", "upper"))
        .withColumn("customer_id", text_col("customer_id", "", "upper"))
        .withColumn("principal_amount", num_col("principal_amount"))
        .dropDuplicates(["loan_id"])
    )

    return branches, customers, accounts, merchants, cards, rates, txns, fraud, loans


def main():
    spark = spark_session()
    branches, customers, accounts, merchants, cards, rates, txns, fraud, loans = clean_core_tables(spark)

    for name, df in {
        "branches": branches,
        "customers": customers,
        "accounts": accounts,
        "merchants": merchants,
        "cards": cards,
        "exchange_rates": rates,
        "transactions": txns,
        "fraud_alerts": fraud,
        "loans": loans,
    }.items():
        write_csv(df, f"{CLEAN}/{name}")

    invalid_txns = txns.join(accounts.select("account_id"), "account_id", "left_anti")
    bad_amount_or_date = txns.filter(F.col("amount").isNull() | F.col("transaction_ts").isNull())
    write_csv(invalid_txns, f"{QUARANTINE}/transactions_invalid_account_id")
    write_csv(bad_amount_or_date, f"{QUARANTINE}/transactions_bad_amount_or_date")

    valid_txns = (
        txns.join(accounts.select("account_id", "customer_id", "branch_id", "account_type"), "account_id", "inner")
        .filter(F.col("amount").isNotNull() & F.col("transaction_ts").isNotNull())
        .join(rates, (txns.currency == rates.currency) & (txns.transaction_date == rates.rate_date), "left")
        .withColumn("usd_rate", F.coalesce(F.col("usd_rate"), F.lit(1.0)))
        .withColumn("amount_usd", F.col("amount") * F.col("usd_rate"))
        .withColumn("signed_amount", F.when(F.col("direction") == "credit", F.abs("amount")).otherwise(-F.abs("amount")))
        .withColumn("signed_amount_usd", F.col("signed_amount") * F.col("usd_rate"))
        .join(merchants.select("merchant_id", "merchant_category", "category_group", "high_risk_flag"), "merchant_id", "left")
    )

    fraud_flags = fraud.groupBy("transaction_id").agg(F.count("alert_id").alias("fraud_alert_count"))
    valid_txns = (
        valid_txns.join(fraud_flags, "transaction_id", "left")
        .fillna({"fraud_alert_count": 0})
        .withColumn("is_large_transaction", F.col("amount_usd") >= F.lit(1000))
        .withColumn("has_fraud_alert", F.col("fraud_alert_count") > 0)
        .withColumn("is_high_risk_transaction", F.col("is_large_transaction") | F.col("has_fraud_alert") | F.coalesce(F.col("high_risk_flag"), F.lit(False)))
    )

    daily_summary = (
        valid_txns.groupBy("transaction_date", "transaction_type", "channel")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount_usd").alias("total_amount_usd"),
            F.sum("signed_amount_usd").alias("net_signed_amount_usd"),
            F.sum(F.col("is_high_risk_transaction").cast("int")).alias("high_risk_transaction_count"),
        )
    )

    merchant_risk = (
        valid_txns.groupBy("merchant_id", "merchant_category", "category_group")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount_usd").alias("total_amount_usd"),
            F.sum("fraud_alert_count").alias("fraud_alert_count"),
            F.sum(F.col("is_high_risk_transaction").cast("int")).alias("high_risk_transaction_count"),
        )
        .withColumn("fraud_alert_rate", F.col("fraud_alert_count") / F.col("transaction_count"))
        .orderBy(F.desc("high_risk_transaction_count"))
    )

    customer_txn = (
        valid_txns.groupBy("customer_id")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount_usd").alias("total_spend_usd"),
            F.sum("signed_amount_usd").alias("net_cashflow_usd"),
            F.sum(F.col("is_high_risk_transaction").cast("int")).alias("high_risk_transaction_count"),
        )
    )

    customer_360 = (
        customers.join(customer_txn, "customer_id", "left")
        .join(loans.groupBy("customer_id").agg(F.count("loan_id").alias("loan_count"), F.sum("principal_amount").alias("total_principal")), "customer_id", "left")
        .fillna({"transaction_count": 0, "total_spend_usd": 0, "net_cashflow_usd": 0, "high_risk_transaction_count": 0, "loan_count": 0, "total_principal": 0})
        .withColumn("risk_score", F.col("high_risk_transaction_count") * F.lit(5) + F.when(F.col("risk_rating") == "High", 20).when(F.col("risk_rating") == "Medium", 10).otherwise(0))
    )

    output_tables = {
        "fact_transactions": valid_txns,
        "daily_transaction_summary": daily_summary,
        "merchant_risk_summary": merchant_risk,
        "customer_360": customer_360,
        "dim_customer_current": customers,
        "dim_account": accounts,
        "dim_merchant": merchants,
        "dim_card": cards,
    }

    for name, df in output_tables.items():
        write_csv(df, f"{MART}/{name}")

    dq = spark.createDataFrame([
        ("transactions_invalid_account_id", invalid_txns.count()),
        ("transactions_bad_amount_or_date", bad_amount_or_date.count()),
    ], ["check_name", "issue_count"])
    write_csv(dq, f"{REPORT}/data_quality_report")

    print("Advanced financial PySpark ETL complete.")
    spark.stop()


if __name__ == "__main__":
    main()
