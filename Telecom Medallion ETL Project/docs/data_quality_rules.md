# Data Quality Rules

- Usage events must reference valid subscribers and cells.
- Billing invoices must reference valid subscribers.
- Payments must reference valid invoices.
- Cell KPIs and alarms must reference valid cells.
- Site outages must reference valid sites.
- Voice duration must be numeric and positive.
- Data-session volume must be numeric and non-negative.
- Invalid rows are preserved in quarantine with a reason.
