# Architecture Notes

## Source Systems

- Core banking: accounts, balances, ACH, wire, branch events
- Card processor: card transactions, cards, chargebacks
- CRM: customers and customer profile updates
- Loan servicing: loans and payments
- Fraud platform: transaction alerts
- Market feed: exchange rates

## Warehouse Model

Dimensions:

- `dim_customer_scd2`
- `dim_customer_current`
- `dim_account`
- `dim_branch`
- `dim_merchant`
- `dim_card`
- `dim_loan`

Facts:

- `fact_transactions`
- `fact_loan_payments`
- `fact_chargebacks`

Marts:

- `customer_360`
- `merchant_risk_summary`
- `daily_transaction_summary`
- `suspicious_activity_report`
- `loan_delinquency_report`
- `card_utilization_report`
- `account_balance_reconciliation`
- `chargeback_summary`

## Incremental Logic

The pipeline writes the latest transaction timestamp to:

```text
logs/run_metadata.json
```

