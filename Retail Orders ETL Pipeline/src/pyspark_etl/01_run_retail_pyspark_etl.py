"""
Retail ETL pipeline using PySpark.
"""

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = str(PROJECT_ROOT / "data" / "raw")
CLEAN_DIR = str(PROJECT_ROOT / "data" / "clean" / "pyspark")
OUTPUT_DIR = str(PROJECT_ROOT / "data" / "output" / "pyspark")
REPORT_DIR = str(PROJECT_ROOT / "reports" / "pyspark")


def spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RetailComplexETL")
        .master("local[*]")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, table: str):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(f"{RAW_DIR}/{table}.csv")
    )


def text_col(column, default="Unknown", case=None):
    cleaned = F.trim(F.coalesce(F.col(column), F.lit("")))
    cleaned = F.when(cleaned == "", F.lit(default)).otherwise(cleaned)
    if case == "upper":
        cleaned = F.upper(cleaned)
    elif case == "lower":
        cleaned = F.lower(cleaned)
    elif case == "title":
        cleaned = F.initcap(cleaned)
    return cleaned


def date_col(column):
    value = F.trim(F.coalesce(F.col(column), F.lit("")))
    return F.coalesce(
        F.to_date(value, "yyyy-MM-dd"),
        F.to_date(value, "yyyy/MM/dd"),
        F.to_date(value, "MM/dd/yyyy"),
        F.to_date(value, "MM-dd-yyyy"),
    )


def num_col(column, default=None):
    value = F.lower(F.trim(F.coalesce(F.col(column), F.lit(""))))
    value = F.regexp_replace(value, "\\$", "")
    value = F.when(value == "free", "0").otherwise(value)
    numeric = value.cast("double")
    if default is not None:
        numeric = F.coalesce(numeric, F.lit(float(default)))
    return numeric


def bool_col(column):
    value = F.upper(F.trim(F.coalesce(F.col(column), F.lit(""))))
    return (
        F.when(value.isin("Y", "YES", "TRUE", "1", "ACTIVE"), F.lit(True))
        .when(value.isin("N", "NO", "FALSE", "0", "INACTIVE"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def write_csv(df, path):
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def clean_tables(spark):
    customers = (
        read_csv(spark, "customers")
        .withColumn("customer_id", text_col("customer_id", default=""))
        .withColumn("full_name", text_col("full_name", case="title"))
        .withColumn("email", text_col("email", default="unknown@example.com", case="lower"))
        .withColumn("phone", text_col("phone"))
        .withColumn("city", text_col("city", case="title"))
        .withColumn("state", text_col("state", case="upper"))
        .withColumn("signup_date", date_col("signup_date"))
        .withColumn("loyalty_tier", text_col("loyalty_tier", case="title"))
        .withColumn("is_active", bool_col("is_active"))
        .dropDuplicates(["customer_id"])
    )

    products = (
        read_csv(spark, "products")
        .withColumn("product_id", text_col("product_id", default=""))
        .withColumn("product_name", text_col("product_name", case="title"))
        .withColumn("category", text_col("category", case="title"))
        .withColumn("subcategory", text_col("subcategory", case="title"))
        .withColumn("cost_price", num_col("cost_price"))
        .withColumn("list_price", num_col("list_price"))
        .withColumn("supplier", text_col("supplier", case="title"))
        .withColumn("active_flag", bool_col("active_flag"))
        .withColumn("launch_date", date_col("launch_date"))
        .dropDuplicates(["product_id"])
    )

    stores = (
        read_csv(spark, "stores")
        .withColumn("store_id", text_col("store_id", default=""))
        .withColumn("store_name", text_col("store_name", case="title"))
        .withColumn("city", text_col("city", case="title"))
        .withColumn("state", text_col("state", case="upper"))
        .withColumn("region", text_col("region", case="title"))
        .withColumn("open_date", date_col("open_date"))
        .dropDuplicates(["store_id"])
    )

    promotions = (
        read_csv(spark, "promotions")
        .withColumn("promo_code", text_col("promo_code", default="", case="upper"))
        .withColumn("campaign_name", text_col("campaign_name", case="title"))
        .withColumn("discount_type", text_col("discount_type", case="lower"))
        .withColumn("discount_value", num_col("discount_value", default=0))
        .withColumn("start_date", date_col("start_date"))
        .withColumn("end_date", date_col("end_date"))
        .dropDuplicates(["promo_code"])
    )

    orders = (
        read_csv(spark, "orders")
        .withColumn("order_id", text_col("order_id", default=""))
        .withColumn("customer_id", text_col("customer_id", default=""))
        .withColumn("store_id", text_col("store_id", default=""))
        .withColumn("order_date", date_col("order_date"))
        .withColumn("order_status_raw", text_col("order_status", case="lower"))
        .withColumn(
            "order_status",
            F.when(F.col("order_status_raw") == "completed", "Completed")
            .when(F.col("order_status_raw") == "shipped", "Shipped")
            .when(F.col("order_status_raw") == "cancelled", "Cancelled")
            .when(F.col("order_status_raw") == "pending", "Pending")
            .otherwise("Unknown")
        )
        .drop("order_status_raw")
        .withColumn("sales_channel_raw", text_col("sales_channel", case="lower"))
        .withColumn(
            "sales_channel",
            F.when(F.col("sales_channel_raw").isin("web", "online"), "Online")
            .when(F.col("sales_channel_raw") == "mobile app", "Mobile App")
            .when(F.col("sales_channel_raw") == "in store", "In Store")
            .otherwise("Unknown")
        )
        .drop("sales_channel_raw")
        .withColumn("currency", F.when(text_col("currency", case="upper") == "US DOLLAR", "USD").otherwise(text_col("currency", case="upper")))
        .withColumn("promo_code", text_col("promo_code", default="", case="upper"))
        .dropDuplicates(["order_id"])
    )

    items = (
        read_csv(spark, "order_items")
        .withColumn("order_item_id", text_col("order_item_id", default=""))
        .withColumn("order_id", text_col("order_id", default=""))
        .withColumn("product_id", text_col("product_id", default=""))
        .withColumn("quantity", num_col("quantity"))
        .withColumn("unit_price", num_col("unit_price"))
        .withColumn("discount_pct", num_col("discount_pct", default=0))
        .withColumn("quantity_valid", F.col("quantity").isNotNull() & (F.col("quantity") > 0))
        .withColumn("unit_price_valid", F.col("unit_price").isNotNull() & (F.col("unit_price") >= 0))
        .withColumn("discount_pct_valid", F.col("discount_pct").between(0, 100))
        .dropDuplicates(["order_item_id"])
    )

    payments = (
        read_csv(spark, "payments")
        .withColumn("payment_id", text_col("payment_id", default=""))
        .withColumn("order_id", text_col("order_id", default=""))
        .withColumn("payment_method_raw", text_col("payment_method", case="lower"))
        .withColumn(
            "payment_method",
            F.when(F.col("payment_method_raw").isin("credit_card", "credit card"), "Credit Card")
            .when(F.col("payment_method_raw") == "debit card", "Debit Card")
            .when(F.col("payment_method_raw") == "paypal", "PayPal")
            .when(F.col("payment_method_raw") == "gift card", "Gift Card")
            .otherwise("Unknown")
        )
        .drop("payment_method_raw")
        .withColumn("payment_status", F.initcap(text_col("payment_status", case="lower")))
        .withColumn("payment_date", date_col("payment_date"))
        .withColumn("amount_paid", num_col("amount_paid"))
        .dropDuplicates(["payment_id"])
    )

    shipments = (
        read_csv(spark, "shipments")
        .withColumn("shipment_id", text_col("shipment_id", default=""))
        .withColumn("order_id", text_col("order_id", default=""))
        .withColumn("carrier", text_col("carrier", case="upper"))
        .withColumn("ship_date", date_col("ship_date"))
        .withColumn("delivery_date", date_col("delivery_date"))
        .withColumn("shipping_cost", num_col("shipping_cost", default=0))
        .withColumn("delivery_status", F.initcap(text_col("delivery_status", case="lower")))
        .withColumn("delivery_days", F.datediff("delivery_date", "ship_date"))
        .withColumn("is_late_delivery", (F.col("delivery_days") > 5) | F.col("delivery_days").isNull() | (F.col("delivery_status") == "Delayed"))
        .dropDuplicates(["shipment_id"])
    )

    returns = (
        read_csv(spark, "returns")
        .withColumn("return_id", text_col("return_id", default=""))
        .withColumn("order_id", text_col("order_id", default=""))
        .withColumn("product_id", text_col("product_id", default=""))
        .withColumn("return_date", date_col("return_date"))
        .withColumn("return_reason", text_col("return_reason", case="title"))
        .withColumn("refund_amount", num_col("refund_amount"))
        .dropDuplicates(["return_id"])
    )

    return {
        "customers": customers,
        "products": products,
        "stores": stores,
        "promotions": promotions,
        "orders": orders,
        "order_items": items,
        "payments": payments,
        "shipments": shipments,
        "returns": returns,
    }


def main():
    spark = spark_session()
    cleaned = clean_tables(spark)

    for name, df in cleaned.items():
        write_csv(df, f"{CLEAN_DIR}/{name}")

    customers = cleaned["customers"]
    products = cleaned["products"]
    stores = cleaned["stores"]
    promotions = cleaned["promotions"]
    orders = cleaned["orders"]
    items = cleaned["order_items"]
    payments = cleaned["payments"]
    shipments = cleaned["shipments"]
    returns = cleaned["returns"]

    valid_items = (
        items
        .join(orders.select("order_id", "order_date", "customer_id", "store_id", "order_status", "promo_code", "sales_channel"), "order_id", "inner")
        .join(products.select("product_id", "product_name", "category", "subcategory", "cost_price"), "product_id", "inner")
        .filter("quantity_valid = true and unit_price_valid = true and discount_pct_valid = true")
        .withColumn("gross_amount", F.col("quantity") * F.col("unit_price"))
        .withColumn("discount_amount", F.col("gross_amount") * (F.col("discount_pct") / F.lit(100)))
        .withColumn("net_sales", F.col("gross_amount") - F.col("discount_amount"))
        .withColumn("estimated_cost", F.col("quantity") * F.coalesce(F.col("cost_price"), F.lit(0)))
        .withColumn("gross_margin", F.col("net_sales") - F.col("estimated_cost"))
        .filter(F.col("order_status").isin("Completed", "Shipped"))
    )

    daily_sales = (
        valid_items.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("gross_margin").alias("gross_margin"),
        )
        .orderBy("order_date")
    )

    category_revenue = (
        valid_items.groupBy("category", "subcategory")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("gross_margin").alias("gross_margin"),
        )
        .orderBy(F.desc("net_sales"))
    )

    customer_lifetime_value = (
        valid_items.groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("units_bought"),
            F.sum("net_sales").alias("lifetime_net_sales"),
        )
        .join(customers.select("customer_id", "full_name", "city", "state", "loyalty_tier"), "customer_id", "left")
        .orderBy(F.desc("lifetime_net_sales"))
    )

    late_shipments = shipments.filter(F.col("is_late_delivery") == True).join(
        orders.select("order_id", "customer_id", "order_date", "sales_channel"), "order_id", "left"
    )

    return_counts = (
        returns.join(orders.select("order_id"), "order_id", "inner")
        .join(products.select("product_id"), "product_id", "inner")
        .groupBy("product_id")
        .agg(F.count("return_id").alias("return_count"), F.sum("refund_amount").alias("refund_amount"))
    )

    sold_counts = valid_items.groupBy("product_id").agg(F.sum("quantity").alias("units_sold"))

    return_rate = (
        products.select("product_id", "product_name", "category")
        .join(sold_counts, "product_id", "left")
        .join(return_counts, "product_id", "left")
        .fillna({"units_sold": 0, "return_count": 0, "refund_amount": 0})
        .withColumn("return_rate", F.when(F.col("units_sold") > 0, F.col("return_count") / F.col("units_sold")).otherwise(F.lit(0)))
        .orderBy(F.desc("return_rate"))
    )

    order_totals = (
        valid_items.groupBy("order_id")
        .agg(F.sum("net_sales").alias("order_net_sales"))
    )

    paid = (
        payments.filter(F.col("payment_status") == "Paid")
        .groupBy("order_id")
        .agg(F.sum("amount_paid").alias("total_paid"))
    )

    payment_reconciliation = (
        orders.select("order_id", "customer_id", "order_status")
        .join(order_totals, "order_id", "left")
        .join(paid, "order_id", "left")
        .fillna({"order_net_sales": 0, "total_paid": 0})
        .withColumn("payment_difference", F.col("total_paid") - F.col("order_net_sales"))
        .withColumn("reconciliation_status", F.when(F.abs(F.col("payment_difference")) <= 0.01, "Matched").otherwise("Mismatch"))
    )

    promo_perf = (
        valid_items.filter(F.col("promo_code") != "")
        .join(promotions.select("promo_code", "campaign_name"), "promo_code", "left")
        .groupBy("promo_code", "campaign_name")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("discount_amount").alias("discount_amount"),
        )
        .orderBy(F.desc("net_sales"))
    )

    output_tables = {
        "dim_customers": customers,
        "dim_products": products,
        "dim_stores": stores,
        "dim_promotions": promotions,
        "fact_orders": orders,
        "fact_order_items": items,
        "fact_payments": payments,
        "fact_shipments": shipments,
        "fact_returns": returns,
        "sales_line_items_enriched": valid_items,
        "daily_sales_summary": daily_sales,
        "category_revenue_summary": category_revenue,
        "customer_lifetime_value": customer_lifetime_value,
        "late_shipment_report": late_shipments,
        "return_rate_by_product": return_rate,
        "payment_reconciliation_report": payment_reconciliation,
        "promotion_performance_report": promo_perf,
    }

    for name, df in output_tables.items():
        write_csv(df, f"{OUTPUT_DIR}/{name}")

    dq_rows = [
        ("orders_with_invalid_customer_id", orders.join(customers, "customer_id", "left_anti").count()),
        ("orders_with_invalid_store_id", orders.join(stores, "store_id", "left_anti").count()),
        ("order_items_with_invalid_order_id", items.join(orders, "order_id", "left_anti").count()),
        ("order_items_with_invalid_product_id", items.join(products, "product_id", "left_anti").count()),
        ("payments_with_invalid_order_id", payments.join(orders, "order_id", "left_anti").count()),
        ("returns_with_invalid_order_id", returns.join(orders, "order_id", "left_anti").count()),
        ("returns_with_invalid_product_id", returns.join(products, "product_id", "left_anti").count()),
        ("order_items_invalid_quantity", items.filter(~F.col("quantity_valid")).count()),
        ("order_items_invalid_unit_price", items.filter(~F.col("unit_price_valid")).count()),
        ("order_items_invalid_discount_pct", items.filter(~F.col("discount_pct_valid")).count()),
    ]
    dq_df = spark.createDataFrame(dq_rows, ["check_name", "issue_count"])
    write_csv(dq_df, f"{REPORT_DIR}/data_quality_checks")

    print("PySpark retail ETL complete.")
    print(f"Clean files: {CLEAN_DIR}")
    print(f"Output files: {OUTPUT_DIR}")
    print(f"Reports: {REPORT_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
