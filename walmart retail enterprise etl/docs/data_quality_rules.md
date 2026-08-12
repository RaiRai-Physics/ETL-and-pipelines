# Data Quality Rules

- Deduplicate all business keys.
- POS transactions must reference valid stores and associates.
- POS and online lines must reference valid products.
- Quantities must be positive.
- Unit prices must be numeric and non-negative.
- Payments must reference existing POS transactions.
- E-commerce orders must reference valid customers and fulfillment stores.
- Inventory snapshots must reference valid stores/products and have valid non-negative on-hand quantities.
- Invalid records are retained in the quarantine layer with a reason.
