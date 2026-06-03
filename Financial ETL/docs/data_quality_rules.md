# Data Quality Rules

## Deduplication

- Deduplicate customers by `customer_id`.
- Deduplicate accounts by `account_id`.
- Deduplicate transactions by `transaction_id`.
- Deduplicate fraud alerts by `alert_id`.
- Deduplicate chargebacks by `chargeback_id`.

## Foreign Key Checks

- `accounts.customer_id` must exist in customers.
- `accounts.branch_id` must exist in branches.
- `cards.account_id` must exist in accounts.
- `transactions.account_id` must exist in accounts.
- `transactions.card_id` must exist in cards when populated.
- `transactions.merchant_id` must exist in merchants when populated.
- `loans.customer_id` must exist in customers.
- `loan_payments.loan_id` must exist in loans.
- `fraud_alerts.transaction_id` must exist in transactions.
- `chargebacks.transaction_id` must exist in transactions.
- `account_daily_balances.account_id` must exist in accounts.

## Validity Checks

- Transaction timestamp must parse successfully.
- Transaction amount must be numeric.
- Currency must have an exchange rate for the transaction date.
- Loan payment status is normalized.
- KYC and risk values are standardized.
- PII is masked in the clean customer table.

## Quarantine Strategy

Invalid rows are not silently deleted. They are written to `data/quarantine/python/` for review.
