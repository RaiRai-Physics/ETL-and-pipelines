# Data Quality Rules

## Primary key checks

- `customers.customer_id` must be unique.
- `products.product_id` must be unique.
- `stores.store_id` must be unique.
- `promotions.promo_code` must be unique.
- `orders.order_id` must be unique.
- `order_items.order_item_id` must be unique.
- `payments.payment_id` must be unique.
- `shipments.shipment_id` must be unique.
- `returns.return_id` must be unique.

## Foreign key checks

- `orders.customer_id` should exist in `customers.customer_id`.
- `orders.store_id` should exist in `stores.store_id`.
- `order_items.order_id` should exist in `orders.order_id`.
- `order_items.product_id` should exist in `products.product_id`.
- `payments.order_id` should exist in `orders.order_id`.
- `shipments.order_id` should exist in `orders.order_id`.
- `returns.order_id` should exist in `orders.order_id`.
- `returns.product_id` should exist in `products.product_id`.

## Standardization rules

- Dates are parsed into ISO format.
- Text values are trimmed.
- Email values are lowercased.
- State values are uppercased.
- Status fields are normalized to consistent labels.
- Boolean flags are standardized.
- Invalid numbers become null unless a safe default is defined.

## Business rules

- Sales reports include only Completed and Shipped orders.
- Cancelled and Pending orders are excluded from sales metrics.
- Invalid order items are excluded from revenue metrics.
- A shipment is late when delivery takes more than 5 days, delivery date is missing, or status is Delayed.
- Payment reconciliation compares paid amount against computed order net sales.
