"""
Retail ETL pipeline using Python + pandas.
Outputs:
    data/clean/python/*.csv
    data/output/python/*.csv
    reports/python/*.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean" / "python"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output" / "python"
REPORT_DIR = PROJECT_ROOT / "reports" / "python"

TRUE_VALUES = {"Y", "YES", "TRUE", "1", "ACTIVE"}
FALSE_VALUES = {"N", "NO", "FALSE", "0", "INACTIVE"}


def read_raw(name: str) -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / f"{name}.csv", dtype=str, keep_default_na=False)


def clean_text(series: pd.Series, default: str = "Unknown", title: bool = False, upper: bool = False, lower: bool = False) -> pd.Series:
    cleaned = series.fillna("").astype(str).str.strip()
    cleaned = cleaned.replace("", default)

    if title:
        cleaned = cleaned.str.title()
    if upper:
        cleaned = cleaned.str.upper()
    if lower:
        cleaned = cleaned.str.lower()

    return cleaned


def parse_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series.replace("", pd.NA), errors="coerce").dt.date.astype("string")


def parse_number(series: pd.Series, default=np.nan) -> pd.Series:
    cleaned = series.fillna("").astype(str).str.strip().str.replace("$", "", regex=False)
    cleaned = cleaned.replace({"": np.nan, "free": "0", "FREE": "0"})
    numeric = pd.to_numeric(cleaned, errors="coerce")
    if not pd.isna(default):
        numeric = numeric.fillna(default)
    return numeric


def standardize_bool(series: pd.Series) -> pd.Series:
    values = series.fillna("").astype(str).str.strip().str.upper()
    return np.where(values.isin(TRUE_VALUES), True, np.where(values.isin(FALSE_VALUES), False, pd.NA))


def write_clean(name: str, df: pd.DataFrame) -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEAN_DIR / f"{name}.csv", index=False)


def write_output(name: str, df: pd.DataFrame) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_DIR / f"{name}.csv", index=False)


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["customer_id"] = clean_text(out["customer_id"], default="")
    out["full_name"] = clean_text(out["full_name"], default="Unknown", title=True)
    out["email"] = clean_text(out["email"], default="unknown@example.com", lower=True)
    out["phone"] = clean_text(out["phone"], default="Unknown")
    out["city"] = clean_text(out["city"], default="Unknown", title=True)
    out["state"] = clean_text(out["state"], default="Unknown", upper=True)
    out["signup_date"] = parse_date(out["signup_date"])
    out["loyalty_tier"] = clean_text(out["loyalty_tier"], default="Unknown", title=True)
    out["is_active"] = standardize_bool(out["is_active"])
    return out.drop_duplicates(subset=["customer_id"], keep="first").reset_index(drop=True)


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["product_id"] = clean_text(out["product_id"], default="")
    out["product_name"] = clean_text(out["product_name"], default="Unknown", title=True)
    out["category"] = clean_text(out["category"], default="Unknown", title=True)
    out["subcategory"] = clean_text(out["subcategory"], default="Unknown", title=True)
    out["cost_price"] = parse_number(out["cost_price"])
    out["list_price"] = parse_number(out["list_price"])
    out["supplier"] = clean_text(out["supplier"], default="Unknown", title=True)
    out["active_flag"] = standardize_bool(out["active_flag"])
    out["launch_date"] = parse_date(out["launch_date"])
    return out.drop_duplicates(subset=["product_id"], keep="first").reset_index(drop=True)


def clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["store_id"] = clean_text(out["store_id"], default="")
    out["store_name"] = clean_text(out["store_name"], default="Unknown", title=True)
    out["city"] = clean_text(out["city"], default="Unknown", title=True)
    out["state"] = clean_text(out["state"], default="Unknown", upper=True)
    out["region"] = clean_text(out["region"], default="Unknown", title=True)
    out["open_date"] = parse_date(out["open_date"])
    return out.drop_duplicates(subset=["store_id"], keep="first").reset_index(drop=True)


def clean_promotions(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["promo_code"] = clean_text(out["promo_code"], default="", upper=True)
    out["campaign_name"] = clean_text(out["campaign_name"], default="Unknown", title=True)
    out["discount_type"] = clean_text(out["discount_type"], default="Unknown", lower=True)
    out["discount_value"] = parse_number(out["discount_value"], default=0)
    out["start_date"] = parse_date(out["start_date"])
    out["end_date"] = parse_date(out["end_date"])
    return out.drop_duplicates(subset=["promo_code"], keep="first").reset_index(drop=True)


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_id"] = clean_text(out["order_id"], default="")
    out["customer_id"] = clean_text(out["customer_id"], default="")
    out["store_id"] = clean_text(out["store_id"], default="")
    out["order_date"] = parse_date(out["order_date"])

    status = clean_text(out["order_status"], default="Unknown", lower=True)
    out["order_status"] = status.replace({
        "completed": "Completed",
        "shipped": "Shipped",
        "cancelled": "Cancelled",
        "pending": "Pending",
    }).str.title()

    channel = clean_text(out["sales_channel"], default="Unknown", lower=True)
    out["sales_channel"] = channel.replace({
        "web": "Online",
        "online": "Online",
        "mobile app": "Mobile App",
        "in store": "In Store",
    }).str.title()

    currency = clean_text(out["currency"], default="Unknown", upper=True)
    out["currency"] = currency.replace({"US DOLLAR": "USD"})
    out["promo_code"] = clean_text(out["promo_code"], default="", upper=True)
    return out.drop_duplicates(subset=["order_id"], keep="first").reset_index(drop=True)


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["order_item_id"] = clean_text(out["order_item_id"], default="")
    out["order_id"] = clean_text(out["order_id"], default="")
    out["product_id"] = clean_text(out["product_id"], default="")
    out["quantity"] = parse_number(out["quantity"])
    out["unit_price"] = parse_number(out["unit_price"])
    out["discount_pct"] = parse_number(out["discount_pct"], default=0)
    out["quantity_valid"] = out["quantity"].notna() & (out["quantity"] > 0)
    out["unit_price_valid"] = out["unit_price"].notna() & (out["unit_price"] >= 0)
    out["discount_pct_valid"] = out["discount_pct"].between(0, 100, inclusive="both")
    return out.drop_duplicates(subset=["order_item_id"], keep="first").reset_index(drop=True)


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["payment_id"] = clean_text(out["payment_id"], default="")
    out["order_id"] = clean_text(out["order_id"], default="")
    method = clean_text(out["payment_method"], default="Unknown", lower=True)
    out["payment_method"] = method.replace({
        "credit_card": "Credit Card",
        "credit card": "Credit Card",
        "debit card": "Debit Card",
        "paypal": "PayPal",
        "gift card": "Gift Card",
    })
    status = clean_text(out["payment_status"], default="Unknown", lower=True)
    out["payment_status"] = status.replace({
        "paid": "Paid",
        "failed": "Failed",
        "refunded": "Refunded",
        "pending": "Pending",
    }).str.title()
    out["payment_date"] = parse_date(out["payment_date"])
    out["amount_paid"] = parse_number(out["amount_paid"])
    return out.drop_duplicates(subset=["payment_id"], keep="first").reset_index(drop=True)


def clean_shipments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["shipment_id"] = clean_text(out["shipment_id"], default="")
    out["order_id"] = clean_text(out["order_id"], default="")
    out["carrier"] = clean_text(out["carrier"], default="Unknown", upper=True)
    out["ship_date"] = parse_date(out["ship_date"])
    out["delivery_date"] = parse_date(out["delivery_date"])
    out["shipping_cost"] = parse_number(out["shipping_cost"], default=0)
    status = clean_text(out["delivery_status"], default="Unknown", lower=True)
    out["delivery_status"] = status.replace({
        "delivered": "Delivered",
        "in transit": "In Transit",
        "delayed": "Delayed",
        "lost": "Lost",
    }).str.title()
    ship_dt = pd.to_datetime(out["ship_date"], errors="coerce")
    delivery_dt = pd.to_datetime(out["delivery_date"], errors="coerce")
    out["delivery_days"] = (delivery_dt - ship_dt).dt.days
    out["is_late_delivery"] = (out["delivery_days"] > 5) | out["delivery_days"].isna() | (out["delivery_status"] == "Delayed")
    return out.drop_duplicates(subset=["shipment_id"], keep="first").reset_index(drop=True)


def clean_returns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["return_id"] = clean_text(out["return_id"], default="")
    out["order_id"] = clean_text(out["order_id"], default="")
    out["product_id"] = clean_text(out["product_id"], default="")
    out["return_date"] = parse_date(out["return_date"])
    out["return_reason"] = clean_text(out["return_reason"], default="Unknown", title=True)
    out["refund_amount"] = parse_number(out["refund_amount"])
    return out.drop_duplicates(subset=["return_id"], keep="first").reset_index(drop=True)


def build_dq_report(cleaned: dict[str, pd.DataFrame]) -> pd.DataFrame:
    customers = cleaned["customers"]
    products = cleaned["products"]
    stores = cleaned["stores"]
    promotions = cleaned["promotions"]
    orders = cleaned["orders"]
    items = cleaned["order_items"]
    payments = cleaned["payments"]
    shipments = cleaned["shipments"]
    returns = cleaned["returns"]

    checks = [
        ("duplicate_customers_removed", len(read_raw("customers")) - len(customers)),
        ("duplicate_products_removed", len(read_raw("products")) - len(products)),
        ("duplicate_orders_removed", len(read_raw("orders")) - len(orders)),
        ("duplicate_order_items_removed", len(read_raw("order_items")) - len(items)),
        ("duplicate_payments_removed", len(read_raw("payments")) - len(payments)),
        ("orders_with_invalid_customer_id", int(~orders["customer_id"].isin(customers["customer_id"]).sum()) if False else int((~orders["customer_id"].isin(customers["customer_id"])).sum())),
        ("orders_with_invalid_store_id", int((~orders["store_id"].isin(stores["store_id"])).sum())),
        ("order_items_with_invalid_order_id", int((~items["order_id"].isin(orders["order_id"])).sum())),
        ("order_items_with_invalid_product_id", int((~items["product_id"].isin(products["product_id"])).sum())),
        ("payments_with_invalid_order_id", int((~payments["order_id"].isin(orders["order_id"])).sum())),
        ("shipments_with_invalid_order_id", int((~shipments["order_id"].isin(orders["order_id"])).sum())),
        ("returns_with_invalid_order_id", int((~returns["order_id"].isin(orders["order_id"])).sum())),
        ("returns_with_invalid_product_id", int((~returns["product_id"].isin(products["product_id"])).sum())),
        ("order_items_invalid_quantity", int((~items["quantity_valid"]).sum())),
        ("order_items_invalid_unit_price", int((~items["unit_price_valid"]).sum())),
        ("order_items_invalid_discount_pct", int((~items["discount_pct_valid"]).sum())),
        ("orders_with_unknown_promo_code", int((orders["promo_code"].ne("") & ~orders["promo_code"].isin(promotions["promo_code"])).sum())),
    ]

    return pd.DataFrame(checks, columns=["check_name", "issue_count"])


def build_outputs(cleaned: dict[str, pd.DataFrame]) -> None:
    customers = cleaned["customers"]
    products = cleaned["products"]
    stores = cleaned["stores"]
    promotions = cleaned["promotions"]
    orders = cleaned["orders"]
    items = cleaned["order_items"]
    payments = cleaned["payments"]
    shipments = cleaned["shipments"]
    returns = cleaned["returns"]

    for name, df in {
        "dim_customers": customers,
        "dim_products": products,
        "dim_stores": stores,
        "dim_promotions": promotions,
        "fact_orders": orders,
        "fact_order_items": items,
        "fact_payments": payments,
        "fact_shipments": shipments,
        "fact_returns": returns,
    }.items():
        write_output(name, df)

    valid_items = items[
        items["order_id"].isin(orders["order_id"])
        & items["product_id"].isin(products["product_id"])
        & items["quantity_valid"]
        & items["unit_price_valid"]
        & items["discount_pct_valid"]
    ].copy()

    valid_items["gross_amount"] = valid_items["quantity"] * valid_items["unit_price"]
    valid_items["discount_amount"] = valid_items["gross_amount"] * (valid_items["discount_pct"] / 100)
    valid_items["net_sales"] = valid_items["gross_amount"] - valid_items["discount_amount"]

    order_sales = (
        valid_items.groupby("order_id", as_index=False)
        .agg(order_gross_amount=("gross_amount", "sum"), order_discount_amount=("discount_amount", "sum"), order_net_sales=("net_sales", "sum"))
    )

    enriched_orders = orders.merge(order_sales, on="order_id", how="left")
    enriched_orders[["order_gross_amount", "order_discount_amount", "order_net_sales"]] = enriched_orders[["order_gross_amount", "order_discount_amount", "order_net_sales"]].fillna(0)
    write_output("fact_orders_enriched", enriched_orders)

    sales_base = (
        valid_items
        .merge(orders[["order_id", "order_date", "customer_id", "store_id", "order_status", "promo_code", "sales_channel"]], on="order_id", how="left")
        .merge(products[["product_id", "product_name", "category", "subcategory", "cost_price"]], on="product_id", how="left")
    )
    sales_base = sales_base[sales_base["order_status"].isin(["Completed", "Shipped"])].copy()
    sales_base["estimated_cost"] = sales_base["quantity"] * sales_base["cost_price"].fillna(0)
    sales_base["gross_margin"] = sales_base["net_sales"] - sales_base["estimated_cost"]
    write_output("sales_line_items_enriched", sales_base)

    daily_sales = (
        sales_base.groupby("order_date", as_index=False)
        .agg(order_count=("order_id", "nunique"), units_sold=("quantity", "sum"), net_sales=("net_sales", "sum"), gross_margin=("gross_margin", "sum"))
        .sort_values("order_date")
    )
    write_output("daily_sales_summary", daily_sales)

    category_revenue = (
        sales_base.groupby(["category", "subcategory"], as_index=False)
        .agg(units_sold=("quantity", "sum"), net_sales=("net_sales", "sum"), gross_margin=("gross_margin", "sum"))
        .sort_values("net_sales", ascending=False)
    )
    write_output("category_revenue_summary", category_revenue)

    customer_lifetime_value = (
        sales_base.groupby("customer_id", as_index=False)
        .agg(order_count=("order_id", "nunique"), units_bought=("quantity", "sum"), lifetime_net_sales=("net_sales", "sum"))
        .merge(customers[["customer_id", "full_name", "city", "state", "loyalty_tier"]], on="customer_id", how="left")
        .sort_values("lifetime_net_sales", ascending=False)
    )
    write_output("customer_lifetime_value", customer_lifetime_value)

    late_shipments = shipments[shipments["is_late_delivery"]].merge(
        orders[["order_id", "customer_id", "order_date", "sales_channel"]], on="order_id", how="left"
    )
    write_output("late_shipment_report", late_shipments)

    product_returns = (
        returns[returns["order_id"].isin(orders["order_id"]) & returns["product_id"].isin(products["product_id"])]
        .groupby("product_id", as_index=False)
        .agg(return_count=("return_id", "count"), refund_amount=("refund_amount", "sum"))
    )
    sold_by_product = sales_base.groupby("product_id", as_index=False).agg(units_sold=("quantity", "sum"))
    return_rate = (
        products[["product_id", "product_name", "category"]]
        .merge(sold_by_product, on="product_id", how="left")
        .merge(product_returns, on="product_id", how="left")
        .fillna({"units_sold": 0, "return_count": 0, "refund_amount": 0})
    )
    return_rate["return_rate"] = np.where(return_rate["units_sold"] > 0, return_rate["return_count"] / return_rate["units_sold"], 0)
    write_output("return_rate_by_product", return_rate.sort_values("return_rate", ascending=False))

    paid = (
        payments[payments["payment_status"] == "Paid"]
        .groupby("order_id", as_index=False)
        .agg(total_paid=("amount_paid", "sum"))
    )
    reconciliation = (
        enriched_orders[["order_id", "customer_id", "order_status", "order_net_sales"]]
        .merge(paid, on="order_id", how="left")
        .fillna({"total_paid": 0})
    )
    reconciliation["payment_difference"] = reconciliation["total_paid"] - reconciliation["order_net_sales"]
    reconciliation["reconciliation_status"] = np.where(reconciliation["payment_difference"].abs() <= 0.01, "Matched", "Mismatch")
    write_output("payment_reconciliation_report", reconciliation.sort_values("reconciliation_status"))

    promo_sales = sales_base.merge(promotions[["promo_code", "campaign_name", "start_date", "end_date"]], on="promo_code", how="left")
    promo_perf = (
        promo_sales[promo_sales["promo_code"].ne("")]
        .groupby(["promo_code", "campaign_name"], dropna=False, as_index=False)
        .agg(order_count=("order_id", "nunique"), net_sales=("net_sales", "sum"), discount_amount=("discount_amount", "sum"))
        .sort_values("net_sales", ascending=False)
    )
    write_output("promotion_performance_report", promo_perf)


def main() -> None:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    cleaned = {
        "customers": clean_customers(read_raw("customers")),
        "products": clean_products(read_raw("products")),
        "stores": clean_stores(read_raw("stores")),
        "promotions": clean_promotions(read_raw("promotions")),
        "orders": clean_orders(read_raw("orders")),
        "order_items": clean_order_items(read_raw("order_items")),
        "payments": clean_payments(read_raw("payments")),
        "shipments": clean_shipments(read_raw("shipments")),
        "returns": clean_returns(read_raw("returns")),
    }

    for name, df in cleaned.items():
        write_clean(name, df)

    dq_report = build_dq_report(cleaned)
    dq_report.to_csv(REPORT_DIR / "data_quality_checks.csv", index=False)

    build_outputs(cleaned)

    print("Python retail ETL complete.")
    print(f"Clean files: {CLEAN_DIR}")
    print(f"Output files: {OUTPUT_DIR}")
    print(f"Reports: {REPORT_DIR}")


if __name__ == "__main__":
    main()
