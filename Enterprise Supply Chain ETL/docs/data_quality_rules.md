# Data Quality Rules

- Deduplicate records by primary business key.
- Validate customer, product, supplier, warehouse, carrier, order, invoice, receipt, and PO references.
- Quarantine invalid references and malformed values.
- Exclude invalid rows from gold fact/mart calculations.
- Convert sales and procurement amounts to USD using FX rates.
- Late delivery is delivery date greater than promised delivery date.
- Supplier fill rate is received quantity divided by ordered quantity.
- AR past due is open amount greater than zero after due date.
