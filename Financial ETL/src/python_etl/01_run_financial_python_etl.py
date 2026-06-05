"""
Advanced Financial ETL Pipeline using Python + pandas.

Run from project root:
    python src/python_etl/01_run_financial_python_etl.py

Pipeline features:
- Raw profiling
- Data cleaning
- PII masking
- Deduplication
- Foreign-key validation
- Transaction currency conversion
- SCD Type 2 customer dimension
- Quarantine outputs
- Fact and dimension tables
- Customer 360 mart
- Fraud/risk marts
- Audit and data quality reports
- Incremental watermark metadata
"""

from pathlib import Path
import sys
import json
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.common.utils import (
    load_config,
    ensure_dirs,
    clean_text,
    parse_date,
    parse_ts,
    parse_number,
    bool_from_text,
    mask_email,
    mask_phone,
    stable_hash,
    write_csv,
    read_raw,
)

CONFIG = load_config()
STAGE_DIR = PROJECT_ROOT / CONFIG["stage_path"]
CLEAN_DIR = PROJECT_ROOT / CONFIG["clean_path"]
QUARANTINE_DIR = PROJECT_ROOT / CONFIG["quarantine_path"]
MART_DIR = PROJECT_ROOT / CONFIG["mart_path"]
REPORT_DIR = PROJECT_ROOT / CONFIG["report_path"]
LOG_DIR = PROJECT_ROOT / "logs"

BASE_CURRENCY = CONFIG["base_currency"]


def save_clean(name: str, df: pd.DataFrame) -> None:
    write_csv(df, CLEAN_DIR / f"{name}.csv")


def save_quarantine(name: str, df: pd.DataFrame) -> None:
    if len(df) > 0:
        write_csv(df, QUARANTINE_DIR / f"{name}.csv")
    else:
        write_csv(pd.DataFrame(columns=["message"]), QUARANTINE_DIR / f"{name}.csv")


def save_mart(name: str, df: pd.DataFrame) -> None:
    write_csv(df, MART_DIR / f"{name}.csv")


def clean_branches(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["branch_id"] = clean_text(out["branch_id"], default="", case="upper")
    out["branch_name"] = clean_text(out["branch_name"], case="title")
    out["city"] = clean_text(out["city"], case="title")
    out["state"] = clean_text(out["state"], case="upper")
    out["region"] = clean_text(out["region"], case="title")
    out["open_date"] = parse_date(out["open_date"])
    out["manager_name"] = clean_text(out["manager_name"], case="title")
    return out.drop_duplicates(subset=["branch_id"], keep="first")


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["customer_id"] = clean_text(out["customer_id"], default="", case="upper")
    out["full_name"] = clean_text(out["full_name"], case="title")
    out["email_raw"] = clean_text(out["email"], default="unknown@example.com", case="lower")
    out["email_masked"] = out["email_raw"].map(mask_email)
    out["phone_masked"] = out["phone"].map(mask_phone)
    out["date_of_birth"] = parse_date(out["date_of_birth"])
    out["signup_date"] = parse_date(out["signup_date"])
    out["customer_segment"] = clean_text(out["customer_segment"], default="Unknown", case="title")
    out["kyc_status"] = clean_text(out["kyc_status"], default="Unknown", case="title")
    out["risk_rating"] = clean_text(out["risk_rating"], default="Unknown", case="title")
    out["city"] = clean_text(out["city"], case="title")
    out["state"] = clean_text(out["state"], case="upper")
    out["country"] = clean_text(out["country"], case="upper").replace({"US": "USA", "UNITED STATES": "USA"})
    out["is_active"] = bool_from_text(out["is_active"])
    out["customer_hash_key"] = out["customer_id"].map(stable_hash)
    out = out.drop(columns=["email", "phone"])
    return out.drop_duplicates(subset=["customer_id"], keep="first")


def clean_profile_updates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["update_id"] = clean_text(out["update_id"], default="", case="upper")
    out["customer_id"] = clean_text(out["customer_id"], default="", case="upper")
    out["effective_date"] = parse_date(out["effective_date"])
    out["new_customer_segment"] = clean_text(out["new_customer_segment"], case="title")
    out["new_risk_rating"] = clean_text(out["new_risk_rating"], case="title")
    out["new_kyc_status"] = clean_text(out["new_kyc_status"], case="title")
    out["source_system"] = clean_text(out["source_system"], case="lower")
    return out.drop_duplicates(subset=["update_id"], keep="first")


def clean_accounts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["account_id"] = clean_text(out["account_id"], default="", case="upper")
    out["customer_id"] = clean_text(out["customer_id"], default="", case="upper")
    out["branch_id"] = clean_text(out["branch_id"], default="", case="upper")
    out["account_type"] = clean_text(out["account_type"], case="title")
    out["currency"] = clean_text(out["currency"], default=BASE_CURRENCY, case="upper")
    out["open_date"] = parse_date(out["open_date"])
    out["close_date"] = parse_date(out["close_date"])
    out["account_status"] = clean_text(out["account_status"], case="title")
    out["opening_balance"] = parse_number(out["opening_balance"], default=0)
    out["overdraft_limit"] = parse_number(out["overdraft_limit"], default=0)
    return out.drop_duplicates(subset=["account_id"], keep="first")


def clean_merchants(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["merchant_id"] = clean_text(out["merchant_id"], default="", case="upper")
    out["merchant_name"] = clean_text(out["merchant_name"], case="title")
    out["merchant_category"] = clean_text(out["merchant_category"], case="title")
    out["category_group"] = clean_text(out["category_group"], case="title")
    out["country"] = clean_text(out["country"], case="upper")
    out["high_risk_flag"] = bool_from_text(out["high_risk_flag"])
    out["onboard_date"] = parse_date(out["onboard_date"])
    return out.drop_duplicates(subset=["merchant_id"], keep="first")


def clean_cards(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["card_id"] = clean_text(out["card_id"], default="", case="upper")
    out["account_id"] = clean_text(out["account_id"], default="", case="upper")
    out["card_type"] = clean_text(out["card_type"], case="title")
    out["card_network"] = clean_text(out["card_network"], case="title")
    out["issue_date"] = parse_date(out["issue_date"])
    out["expiry_date"] = parse_date(out["expiry_date"])
    out["card_status"] = clean_text(out["card_status"], case="title")
    out["credit_limit"] = parse_number(out["credit_limit"], default=0)
    return out.drop_duplicates(subset=["card_id"], keep="first")


def clean_exchange_rates(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["rate_date"] = parse_date(out["rate_date"])
    out["currency"] = clean_text(out["currency"], default=BASE_CURRENCY, case="upper")
    out["usd_rate"] = parse_number(out["usd_rate"])
    out["source_system"] = clean_text(out["source_system"], case="lower")
    return out.drop_duplicates(subset=["rate_date", "currency"], keep="last")


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["transaction_id"] = clean_text(out["transaction_id"], default="", case="upper")
    out["account_id"] = clean_text(out["account_id"], default="", case="upper")
    out["card_id"] = clean_text(out["card_id"], default="", case="upper")
    out["merchant_id"] = clean_text(out["merchant_id"], default="", case="upper")
    out["transaction_ts"] = parse_ts(out["transaction_ts"])
    out["transaction_date"] = out["transaction_ts"].dt.date.astype("string")
    out["transaction_type"] = clean_text(out["transaction_type"], case="upper")
    out["channel"] = clean_text(out["channel"], case="title").replace({"Online": "Online"})
    out["direction"] = clean_text(out["direction"], case="lower")
    out["amount"] = parse_number(out["amount"])
    out["currency"] = clean_text(out["currency"], default=BASE_CURRENCY, case="upper")
    out["transaction_status"] = clean_text(out["transaction_status"], case="title")
    out["description"] = clean_text(out["description"], case="title")
    out["source_system"] = clean_text(out["source_system"], case="lower")
    out["ingestion_batch_id"] = clean_text(out["ingestion_batch_id"], default="UNKNOWN", case="upper")
    return out.drop_duplicates(subset=["transaction_id"], keep="first")


def clean_loans(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["loan_id"] = clean_text(out["loan_id"], default="", case="upper")
    out["customer_id"] = clean_text(out["customer_id"], default="", case="upper")
    out["branch_id"] = clean_text(out["branch_id"], default="", case="upper")
    out["loan_type"] = clean_text(out["loan_type"], case="title")
    out["origination_date"] = parse_date(out["origination_date"])
    out["principal_amount"] = parse_number(out["principal_amount"])
    out["interest_rate"] = parse_number(out["interest_rate"])
    out["term_months"] = parse_number(out["term_months"])
    out["loan_status"] = clean_text(out["loan_status"], case="title")
    return out.drop_duplicates(subset=["loan_id"], keep="first")


def clean_loan_payments(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["loan_payment_id"] = clean_text(out["loan_payment_id"], default="", case="upper")
    out["loan_id"] = clean_text(out["loan_id"], default="", case="upper")
    out["due_date"] = parse_date(out["due_date"])
    out["paid_date"] = parse_date(out["paid_date"])
    out["scheduled_amount"] = parse_number(out["scheduled_amount"])
    out["paid_amount"] = parse_number(out["paid_amount"], default=0)
    out["payment_status"] = clean_text(out["payment_status"], case="title")
    due = pd.to_datetime(out["due_date"], errors="coerce")
    paid = pd.to_datetime(out["paid_date"], errors="coerce")
    out["days_late"] = (paid - due).dt.days
    out["is_late"] = out["days_late"].fillna(999) > CONFIG["late_payment_days"]
    return out.drop_duplicates(subset=["loan_payment_id"], keep="first")


def clean_credit_scores(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["customer_id"] = clean_text(out["customer_id"], default="", case="upper")
    out["score_date"] = parse_date(out["score_date"])
    out["credit_score"] = parse_number(out["credit_score"])
    out["bureau"] = clean_text(out["bureau"], case="title")
    return out.drop_duplicates(subset=["customer_id", "score_date", "bureau"], keep="last")


def clean_fraud_alerts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["alert_id"] = clean_text(out["alert_id"], default="", case="upper")
    out["transaction_id"] = clean_text(out["transaction_id"], default="", case="upper")
    out["alert_ts"] = parse_ts(out["alert_ts"])
    out["rule_name"] = clean_text(out["rule_name"], case="lower")
    out["alert_severity"] = clean_text(out["alert_severity"], case="title")
    out["review_status"] = clean_text(out["review_status"], case="title")
    out["analyst_id"] = clean_text(out["analyst_id"], default="Unassigned", case="upper")
    return out.drop_duplicates(subset=["alert_id"], keep="first")


def clean_chargebacks(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["chargeback_id"] = clean_text(out["chargeback_id"], default="", case="upper")
    out["transaction_id"] = clean_text(out["transaction_id"], default="", case="upper")
    out["chargeback_date"] = parse_date(out["chargeback_date"])
    out["reason_code"] = clean_text(out["reason_code"], case="title")
    out["chargeback_amount"] = parse_number(out["chargeback_amount"])
    out["case_status"] = clean_text(out["case_status"], case="title")
    return out.drop_duplicates(subset=["chargeback_id"], keep="first")


def clean_account_balances(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["account_id"] = clean_text(out["account_id"], default="", case="upper")
    out["balance_date"] = parse_date(out["balance_date"])
    out["ledger_balance"] = parse_number(out["ledger_balance"])
    out["available_balance"] = parse_number(out["available_balance"])
    out["currency"] = clean_text(out["currency"], default=BASE_CURRENCY, case="upper")
    return out.drop_duplicates(subset=["account_id", "balance_date"], keep="last")


def build_customer_scd2(customers: pd.DataFrame, updates: pd.DataFrame) -> pd.DataFrame:
    base_rows = customers[[
        "customer_id", "full_name", "email_masked", "phone_masked", "customer_segment",
        "kyc_status", "risk_rating", "city", "state", "country", "signup_date", "is_active"
    ]].copy()
    base_rows["effective_start_date"] = base_rows["signup_date"].fillna("1900-01-01")
    base_rows["record_source"] = "customer_master"

    update_rows = updates.merge(
        customers[["customer_id", "full_name", "email_masked", "phone_masked", "city", "state", "country", "signup_date", "is_active"]],
        on="customer_id",
        how="inner",
    )
    update_rows = update_rows.rename(columns={
        "new_customer_segment": "customer_segment",
        "new_kyc_status": "kyc_status",
        "new_risk_rating": "risk_rating",
        "effective_date": "effective_start_date",
        "source_system": "record_source",
    })
    update_rows = update_rows[base_rows.columns]

    scd = pd.concat([base_rows, update_rows], ignore_index=True)
    scd["effective_start_date_dt"] = pd.to_datetime(scd["effective_start_date"], errors="coerce")
    scd = scd.sort_values(["customer_id", "effective_start_date_dt"])
    scd["effective_end_date"] = (
        scd.groupby("customer_id")["effective_start_date_dt"]
        .shift(-1)
        .dt.date
        .astype("string")
    )
    scd["effective_end_date"] = scd["effective_end_date"].fillna("9999-12-31")
    scd["is_current"] = scd["effective_end_date"].eq("9999-12-31")
    scd["customer_sk"] = [
        stable_hash(f"{cid}|{start}|{seg}|{risk}")
        for cid, start, seg, risk in zip(scd["customer_id"], scd["effective_start_date"], scd["customer_segment"], scd["risk_rating"])
    ]
    scd = scd.drop(columns=["effective_start_date_dt"])
    return scd[[
        "customer_sk", "customer_id", "full_name", "email_masked", "phone_masked",
        "customer_segment", "kyc_status", "risk_rating", "city", "state", "country",
        "signup_date", "is_active", "effective_start_date", "effective_end_date",
        "is_current", "record_source"
    ]]


def build_data_quality_report(raw_counts: dict[str, int], clean: dict[str, pd.DataFrame], quarantine_counts: dict[str, int]) -> pd.DataFrame:
    checks = []
    for table, raw_count in raw_counts.items():
        clean_name = table
        if clean_name in clean:
            checks.append({
                "check_name": f"{table}_raw_to_clean_row_difference",
                "table_name": table,
                "raw_count": raw_count,
                "clean_count": len(clean[clean_name]),
                "issue_count": raw_count - len(clean[clean_name]),
            })

    checks.extend([
        {"check_name": name, "table_name": name.split("_")[0], "raw_count": None, "clean_count": None, "issue_count": count}
        for name, count in quarantine_counts.items()
    ])
    return pd.DataFrame(checks)


def main() -> None:
    ensure_dirs(STAGE_DIR, CLEAN_DIR, QUARANTINE_DIR, MART_DIR, REPORT_DIR, LOG_DIR)

    raw_names = [
        "branches", "customers", "customer_profile_updates", "accounts", "merchants", "cards",
        "exchange_rates", "transactions", "loans", "loan_payments", "credit_scores",
        "fraud_alerts", "chargebacks", "account_daily_balances"
    ]

    raw_counts = {name: len(read_raw(name)) for name in raw_names}

    clean = {
        "branches": clean_branches(read_raw("branches")),
        "customers": clean_customers(read_raw("customers")),
        "customer_profile_updates": clean_profile_updates(read_raw("customer_profile_updates")),
        "accounts": clean_accounts(read_raw("accounts")),
        "merchants": clean_merchants(read_raw("merchants")),
        "cards": clean_cards(read_raw("cards")),
        "exchange_rates": clean_exchange_rates(read_raw("exchange_rates")),
        "transactions": clean_transactions(read_raw("transactions")),
        "loans": clean_loans(read_raw("loans")),
        "loan_payments": clean_loan_payments(read_raw("loan_payments")),
        "credit_scores": clean_credit_scores(read_raw("credit_scores")),
        "fraud_alerts": clean_fraud_alerts(read_raw("fraud_alerts")),
        "chargebacks": clean_chargebacks(read_raw("chargebacks")),
        "account_daily_balances": clean_account_balances(read_raw("account_daily_balances")),
    }

    for name, df in clean.items():
        save_clean(name, df)

    customers = clean["customers"]
    branches = clean["branches"]
    accounts = clean["accounts"]
    merchants = clean["merchants"]
    cards = clean["cards"]
    rates = clean["exchange_rates"]
    txns = clean["transactions"]
    loans = clean["loans"]
    loan_payments = clean["loan_payments"]
    credit_scores = clean["credit_scores"]
    fraud_alerts = clean["fraud_alerts"]
    chargebacks = clean["chargebacks"]
    balances = clean["account_daily_balances"]

    quarantine = {}

    quarantine["accounts_invalid_customer_id"] = accounts[~accounts["customer_id"].isin(customers["customer_id"])]
    quarantine["accounts_invalid_branch_id"] = accounts[~accounts["branch_id"].isin(branches["branch_id"])]
    quarantine["cards_invalid_account_id"] = cards[~cards["account_id"].isin(accounts["account_id"])]
    quarantine["transactions_invalid_account_id"] = txns[~txns["account_id"].isin(accounts["account_id"])]
    quarantine["transactions_invalid_card_id"] = txns[(txns["card_id"] != "") & (~txns["card_id"].isin(cards["card_id"]))]
    quarantine["transactions_invalid_merchant_id"] = txns[(txns["merchant_id"] != "") & (~txns["merchant_id"].isin(merchants["merchant_id"]))]
    quarantine["transactions_bad_amount_or_date"] = txns[txns["amount"].isna() | txns["transaction_ts"].isna()]
    quarantine["loans_invalid_customer_id"] = loans[~loans["customer_id"].isin(customers["customer_id"])]
    quarantine["loan_payments_invalid_loan_id"] = loan_payments[~loan_payments["loan_id"].isin(loans["loan_id"])]
    quarantine["fraud_alerts_invalid_transaction_id"] = fraud_alerts[~fraud_alerts["transaction_id"].isin(txns["transaction_id"])]
    quarantine["chargebacks_invalid_transaction_id"] = chargebacks[~chargebacks["transaction_id"].isin(txns["transaction_id"])]
    quarantine["balances_invalid_account_id"] = balances[~balances["account_id"].isin(accounts["account_id"])]

    for name, df in quarantine.items():
        save_quarantine(name, df)

    quarantine_counts = {name: len(df) for name, df in quarantine.items()}

    customer_scd2 = build_customer_scd2(customers, clean["customer_profile_updates"])
    save_mart("dim_customer_scd2", customer_scd2)
    save_mart("dim_customer_current", customer_scd2[customer_scd2["is_current"]].copy())
    save_mart("dim_account", accounts)
    save_mart("dim_branch", branches)
    save_mart("dim_merchant", merchants)
    save_mart("dim_card", cards)
    save_mart("dim_loan", loans)

    valid_txns = txns[
        txns["account_id"].isin(accounts["account_id"])
        & txns["amount"].notna()
        & txns["transaction_ts"].notna()
    ].copy()

    valid_txns["rate_date"] = valid_txns["transaction_date"]
    txns_with_rates = valid_txns.merge(
        rates[["rate_date", "currency", "usd_rate"]],
        left_on=["rate_date", "currency"],
        right_on=["rate_date", "currency"],
        how="left",
    )

    missing_rate = txns_with_rates[txns_with_rates["usd_rate"].isna()]
    save_quarantine("transactions_missing_exchange_rate", missing_rate)

    txns_with_rates["usd_rate"] = txns_with_rates["usd_rate"].fillna(1)
    txns_with_rates["signed_amount"] = np.where(
        txns_with_rates["direction"].eq("credit"),
        txns_with_rates["amount"].abs(),
        -txns_with_rates["amount"].abs()
    )
    txns_with_rates["amount_usd"] = txns_with_rates["amount"] * txns_with_rates["usd_rate"]
    txns_with_rates["signed_amount_usd"] = txns_with_rates["signed_amount"] * txns_with_rates["usd_rate"]
    txns_with_rates["is_large_transaction"] = txns_with_rates["amount_usd"] >= CONFIG["large_transaction_usd_threshold"]

    txns_enriched = (
        txns_with_rates
        .merge(accounts[["account_id", "customer_id", "branch_id", "account_type"]], on="account_id", how="left")
        .merge(merchants[["merchant_id", "merchant_category", "category_group", "high_risk_flag"]], on="merchant_id", how="left")
    )

    fraud_flags = fraud_alerts.groupby("transaction_id", as_index=False).agg(
        fraud_alert_count=("alert_id", "count"),
        max_alert_severity=("alert_severity", lambda s: "Critical" if "Critical" in set(s) else ("High" if "High" in set(s) else s.iloc[0])),
    )
    txns_enriched = txns_enriched.merge(fraud_flags, on="transaction_id", how="left")
    txns_enriched["fraud_alert_count"] = txns_enriched["fraud_alert_count"].fillna(0).astype(int)
    txns_enriched["has_fraud_alert"] = txns_enriched["fraud_alert_count"] > 0
    txns_enriched["is_high_risk_transaction"] = (
        txns_enriched["is_large_transaction"]
        | txns_enriched["has_fraud_alert"]
        | txns_enriched["high_risk_flag"].fillna(False).astype(bool)
    )
    save_mart("fact_transactions", txns_enriched)

    # Balances and reconciliation
    latest_balances = balances.sort_values("balance_date").groupby("account_id", as_index=False).tail(1)
    txn_net_by_account = txns_enriched.groupby("account_id", as_index=False).agg(
        net_transaction_amount_usd=("signed_amount_usd", "sum"),
        transaction_count=("transaction_id", "count"),
        large_transaction_count=("is_large_transaction", "sum"),
        high_risk_transaction_count=("is_high_risk_transaction", "sum"),
    )
    account_recon = accounts.merge(latest_balances, on="account_id", how="left", suffixes=("", "_latest"))
    account_recon = account_recon.merge(txn_net_by_account, on="account_id", how="left").fillna({
        "net_transaction_amount_usd": 0,
        "transaction_count": 0,
        "large_transaction_count": 0,
        "high_risk_transaction_count": 0,
    })
    account_recon["computed_balance_estimate"] = account_recon["opening_balance"] + account_recon["net_transaction_amount_usd"]
    account_recon["balance_difference_estimate"] = account_recon["ledger_balance"].fillna(0) - account_recon["computed_balance_estimate"]
    save_mart("account_balance_reconciliation", account_recon)

    # Loan marts
    loan_payments_valid = loan_payments[loan_payments["loan_id"].isin(loans["loan_id"])].copy()
    loan_payment_summary = loan_payments_valid.groupby("loan_id", as_index=False).agg(
        scheduled_amount_total=("scheduled_amount", "sum"),
        paid_amount_total=("paid_amount", "sum"),
        missed_or_late_payment_count=("is_late", "sum"),
        payment_count=("loan_payment_id", "count"),
        max_days_late=("days_late", "max"),
    )
    loan_delinquency = loans.merge(loan_payment_summary, on="loan_id", how="left").fillna({
        "scheduled_amount_total": 0,
        "paid_amount_total": 0,
        "missed_or_late_payment_count": 0,
        "payment_count": 0,
        "max_days_late": 0,
    })
    loan_delinquency["outstanding_payment_gap"] = loan_delinquency["scheduled_amount_total"] - loan_delinquency["paid_amount_total"]
    loan_delinquency["delinquency_bucket"] = pd.cut(
        loan_delinquency["max_days_late"],
        bins=[-1, 0, 10, 30, 999],
        labels=["Current", "1-10 Days", "11-30 Days", "30+ Days"]
    ).astype("string")
    save_mart("loan_delinquency_report", loan_delinquency)
    save_mart("fact_loan_payments", loan_payments_valid)

    # Card utilization
    card_accounts = cards.merge(accounts[["account_id", "customer_id"]], on="account_id", how="left")
    card_spend = txns_enriched[txns_enriched["card_id"] != ""].groupby("card_id", as_index=False).agg(
        card_spend_usd=("amount_usd", "sum"),
        transaction_count=("transaction_id", "count"),
    )
    card_util = card_accounts.merge(card_spend, on="card_id", how="left").fillna({
        "card_spend_usd": 0,
        "transaction_count": 0,
    })
    card_util["credit_utilization_ratio"] = np.where(card_util["credit_limit"] > 0, card_util["card_spend_usd"] / card_util["credit_limit"], 0)
    save_mart("card_utilization_report", card_util)

    # Fraud and chargeback marts
    cb_valid = chargebacks[chargebacks["transaction_id"].isin(txns["transaction_id"])].copy()
    save_mart("fact_chargebacks", cb_valid)
    chargeback_summary = (
        cb_valid.merge(txns_enriched[["transaction_id", "customer_id", "merchant_id", "amount_usd"]], on="transaction_id", how="left")
        .groupby(["merchant_id", "reason_code"], dropna=False, as_index=False)
        .agg(chargeback_count=("chargeback_id", "count"), chargeback_amount=("chargeback_amount", "sum"))
        .sort_values("chargeback_count", ascending=False)
    )
    save_mart("chargeback_summary", chargeback_summary)

    suspicious_activity = txns_enriched[
        txns_enriched["is_high_risk_transaction"]
        | (txns_enriched["amount_usd"] > CONFIG["large_transaction_usd_threshold"] * 2)
    ].copy()
    suspicious_activity = suspicious_activity.sort_values(["amount_usd", "fraud_alert_count"], ascending=[False, False])
    save_mart("suspicious_activity_report", suspicious_activity)

    merchant_risk = txns_enriched.groupby(["merchant_id", "merchant_category", "category_group"], dropna=False, as_index=False).agg(
        transaction_count=("transaction_id", "count"),
        total_amount_usd=("amount_usd", "sum"),
        fraud_alert_count=("fraud_alert_count", "sum"),
        high_risk_transaction_count=("is_high_risk_transaction", "sum"),
    )
    merchant_risk["fraud_alert_rate"] = np.where(
        merchant_risk["transaction_count"] > 0,
        merchant_risk["fraud_alert_count"] / merchant_risk["transaction_count"],
        0,
    )
    save_mart("merchant_risk_summary", merchant_risk.sort_values("high_risk_transaction_count", ascending=False))

    # Daily mart
    daily_txn = txns_enriched.groupby(["transaction_date", "transaction_type", "channel"], as_index=False).agg(
        transaction_count=("transaction_id", "count"),
        total_amount_usd=("amount_usd", "sum"),
        net_signed_amount_usd=("signed_amount_usd", "sum"),
        high_risk_transaction_count=("is_high_risk_transaction", "sum"),
    )
    save_mart("daily_transaction_summary", daily_txn.sort_values("transaction_date"))

    # Latest credit score
    latest_scores = credit_scores.dropna(subset=["credit_score"]).sort_values("score_date").groupby("customer_id", as_index=False).tail(1)

    customer_txn = txns_enriched.groupby("customer_id", as_index=False).agg(
        transaction_count=("transaction_id", "count"),
        total_spend_usd=("amount_usd", "sum"),
        net_cashflow_usd=("signed_amount_usd", "sum"),
        large_transaction_count=("is_large_transaction", "sum"),
        high_risk_transaction_count=("is_high_risk_transaction", "sum"),
    )
    customer_loans = loans.groupby("customer_id", as_index=False).agg(
        loan_count=("loan_id", "count"),
        total_principal=("principal_amount", "sum"),
    )
    customer_accounts = accounts.groupby("customer_id", as_index=False).agg(
        account_count=("account_id", "count"),
    )
    customer_360 = (
        customers
        .merge(customer_accounts, on="customer_id", how="left")
        .merge(customer_txn, on="customer_id", how="left")
        .merge(customer_loans, on="customer_id", how="left")
        .merge(latest_scores[["customer_id", "credit_score", "score_date"]], on="customer_id", how="left")
        .fillna({
            "account_count": 0,
            "transaction_count": 0,
            "total_spend_usd": 0,
            "net_cashflow_usd": 0,
            "large_transaction_count": 0,
            "high_risk_transaction_count": 0,
            "loan_count": 0,
            "total_principal": 0,
        })
    )
    customer_360["risk_score"] = (
        customer_360["high_risk_transaction_count"] * 5
        + customer_360["large_transaction_count"] * 2
        + np.where(customer_360["risk_rating"].eq("High"), 20, np.where(customer_360["risk_rating"].eq("Medium"), 10, 0))
        + np.where(customer_360["kyc_status"].ne("Verified"), 10, 0)
    )
    save_mart("customer_360", customer_360.sort_values("risk_score", ascending=False))

    dq_report = build_data_quality_report(raw_counts, clean, quarantine_counts | {"transactions_missing_exchange_rate": len(missing_rate)})
    write_csv(dq_report, REPORT_DIR / "data_quality_report.csv")

    audit_log = pd.DataFrame([{
        "pipeline_name": CONFIG["project_name"],
        "run_timestamp": pd.Timestamp.utcnow().isoformat(),
        "raw_table_count": len(raw_names),
        "raw_total_rows": sum(raw_counts.values()),
        "clean_total_rows": sum(len(df) for df in clean.values()),
        "quarantine_total_rows": sum(quarantine_counts.values()) + len(missing_rate),
        "mart_count": len(list(MART_DIR.glob("*.csv"))),
        "status": "SUCCESS",
    }])
    write_csv(audit_log, REPORT_DIR / "pipeline_audit_log.csv")

    watermark_value = txns["transaction_ts"].max()
    metadata = {
        "last_successful_run_utc": pd.Timestamp.utcnow().isoformat(),
        "transaction_watermark": None if pd.isna(watermark_value) else watermark_value.isoformat(),
        "status": "SUCCESS"
    }
    (LOG_DIR / "run_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    print("Advanced financial Python ETL complete.")
    print(f"Clean layer: {CLEAN_DIR}")
    print(f"Quarantine layer: {QUARANTINE_DIR}")
    print(f"Mart layer: {MART_DIR}")
    print(f"Reports: {REPORT_DIR}")


if __name__ == "__main__":
    main()
