-- Warehouse-style DDL examples

CREATE TABLE dim_customer_scd2 (
    customer_sk VARCHAR,
    customer_id VARCHAR,
    full_name VARCHAR,
    email_masked VARCHAR,
    phone_masked VARCHAR,
    customer_segment VARCHAR,
    kyc_status VARCHAR,
    risk_rating VARCHAR,
    city VARCHAR,
    state VARCHAR,
    country VARCHAR,
    signup_date DATE,
    is_active BOOLEAN,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN,
    record_source VARCHAR
);

CREATE TABLE fact_transactions (
    transaction_id VARCHAR,
    account_id VARCHAR,
    customer_id VARCHAR,
    branch_id VARCHAR,
    card_id VARCHAR,
    merchant_id VARCHAR,
    transaction_ts TIMESTAMP,
    transaction_date DATE,
    transaction_type VARCHAR,
    channel VARCHAR,
    direction VARCHAR,
    amount DECIMAL(18,2),
    currency VARCHAR,
    usd_rate DECIMAL(18,6),
    amount_usd DECIMAL(18,2),
    signed_amount_usd DECIMAL(18,2),
    transaction_status VARCHAR,
    is_large_transaction BOOLEAN,
    has_fraud_alert BOOLEAN,
    is_high_risk_transaction BOOLEAN
);
