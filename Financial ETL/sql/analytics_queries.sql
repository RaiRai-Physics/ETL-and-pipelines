-- 1. Highest-risk customers
SELECT
    customer_id,
    full_name,
    customer_segment,
    risk_rating,
    transaction_count,
    high_risk_transaction_count,
    risk_score
FROM customer_360
ORDER BY risk_score DESC;

-- 2. Daily transaction volume by channel
SELECT
    transaction_date,
    channel,
    SUM(transaction_count) AS transaction_count,
    SUM(total_amount_usd) AS total_amount_usd
FROM daily_transaction_summary
GROUP BY transaction_date, channel
ORDER BY transaction_date, channel;

-- 3. Merchants with most high-risk transactions
SELECT
    merchant_id,
    merchant_category,
    transaction_count,
    total_amount_usd,
    fraud_alert_count,
    high_risk_transaction_count,
    fraud_alert_rate
FROM merchant_risk_summary
ORDER BY high_risk_transaction_count DESC;

-- 4. Loan delinquency
SELECT
    loan_id,
    customer_id,
    loan_type,
    principal_amount,
    missed_or_late_payment_count,
    max_days_late,
    delinquency_bucket
FROM loan_delinquency_report
ORDER BY max_days_late DESC;

-- 5. Account reconciliation outliers
SELECT
    account_id,
    customer_id,
    opening_balance,
    ledger_balance,
    computed_balance_estimate,
    balance_difference_estimate
FROM account_balance_reconciliation
ORDER BY ABS(balance_difference_estimate) DESC;
