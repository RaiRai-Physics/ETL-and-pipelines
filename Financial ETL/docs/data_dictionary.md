# Data Dictionary

## Core Dimensions

### branches
Bank branch reference data.

### customers
Customer master data with masked PII in the clean layer.

### customer_profile_updates
Change records used to build SCD Type 2 customer dimension.

### accounts
Bank accounts linked to customers and branches.

### merchants
Merchant reference data used for card/POS transaction enrichment.

### cards
Debit, credit, and prepaid card reference data.

### loans
Loan contracts linked to customers.

## Core Facts

### transactions
Bank/card transaction records with timestamps, amounts, currency, status, source system, and channel.

### loan_payments
Scheduled and actual loan payment records.

### fraud_alerts
Transaction-level fraud monitoring alerts.

### chargebacks
Chargeback and customer dispute records.

### account_daily_balances
Account ledger and available balance snapshots.

## External/Reference

### exchange_rates
Daily exchange rates used to convert transaction amounts to USD.
