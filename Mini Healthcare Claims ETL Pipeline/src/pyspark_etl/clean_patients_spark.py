from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, upper, count


RAW_FILE = "data/raw/patients.csv"
CLEAN_OUTPUT = "data/output/patients_clean_spark"
REPORT_OUTPUT = "reports/pyspark_data_quality_report"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("ETL PySpark Refresher")
        .master("local[*]")
        .getOrCreate()
    )


def standardize_patients(df):
    cleaned_df = (
        df
        .withColumn("patient_id", trim(col("patient_id")))
        .withColumn(
            "name",
            when(trim(col("name")) == "", "NA")
            .when(col("name").isNull(), "NA")
            .otherwise(trim(col("name")))
        )
        .withColumn(
            "age",
            when(trim(col("age")) == "", "NA")
            .when(col("age").isNull(), "NA")
            .otherwise(trim(col("age")))
        )
        .withColumn(
            "gender",
            when(upper(trim(col("gender"))).isin("M", "MALE"), "Male")
            .when(upper(trim(col("gender"))).isin("F", "FEMALE"), "Female")
            .otherwise("NA")
        )
        .withColumn(
            "city",
            when(trim(col("city")) == "", "NA")
            .when(col("city").isNull(), "NA")
            .otherwise(trim(col("city")))
        )
        .withColumn(
            "disease",
            when(trim(col("disease")) == "", "NA")
            .when(col("disease").isNull(), "NA")
            .otherwise(trim(col("disease")))
        )
    )

    cleaned_df = cleaned_df.dropDuplicates(["patient_id"])

    return cleaned_df


def create_quality_report(raw_df, cleaned_df):
    raw_count = raw_df.count()
    clean_count = cleaned_df.count()
    duplicate_count = raw_count - clean_count

    missing_name_count = cleaned_df.filter(col("name") == "NA").count()
    missing_age_count = cleaned_df.filter(col("age") == "NA").count()
    missing_gender_count = cleaned_df.filter(col("gender") == "NA").count()

    report_data = [
        ("Raw row count", raw_count),
        ("Clean row count", clean_count),
        ("Duplicate rows removed", duplicate_count),
        ("Missing names replaced", missing_name_count),
        ("Missing ages replaced", missing_age_count),
        ("Missing genders replaced", missing_gender_count),
    ]

    return report_data


def main():
    spark = create_spark_session()

    raw_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(RAW_FILE)
    )

    print("Raw data:")
    raw_df.show()

    cleaned_df = standardize_patients(raw_df)

    print("Cleaned data:")
    cleaned_df.show()

    cleaned_df.write.mode("overwrite").option("header", True).csv(CLEAN_OUTPUT)

    report_data = create_quality_report(raw_df, cleaned_df)
    report_df = spark.createDataFrame(report_data, ["metric", "value"])

    report_df.show()
    report_df.write.mode("overwrite").option("header", True).csv(REPORT_OUTPUT)

    print("PySpark ETL completed successfully.")
    print(f"Clean output written to: {CLEAN_OUTPUT}")
    print(f"Report written to: {REPORT_OUTPUT}")

    spark.stop()


if __name__ == "__main__":
    main()
