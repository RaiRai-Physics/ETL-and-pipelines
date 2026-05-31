# Data Dictionary

## customers.csv

| Column | Description |
|---|---|
| customer_id | Unique customer identifier |
| full_name | Customer name |
| email | Customer email |
| phone | Customer phone |
| city | Customer city |
| state | Customer state |
| signup_date | Date customer signed up |
| loyalty_tier | Bronze, Silver, Gold, Platinum, or Unknown |
| is_active | Customer activity flag |

## products.csv

| Column | Description |
|---|---|
| product_id | Unique product identifier |
| product_name | Product name |
| category | Product category |
| subcategory | Product subcategory |
| cost_price | Product cost to company |
| list_price | Product selling price |
| supplier | Supplier name |
| active_flag | Whether product is active |
| launch_date | Product launch date |

## orders.csv

| Column | Description |
|---|---|
| order_id | Unique order identifier |
| customer_id | Customer placing the order |
| store_id | Store or online channel |
| order_date | Order date |
| order_status | Completed, Shipped, Cancelled, Pending |
| sales_channel | Online, In Store, Mobile App |
| currency | Currency code |
| promo_code | Promotion code applied |

## order_items.csv

| Column | Description |
|---|---|
| order_item_id | Unique line item identifier |
| order_id | Parent order |
| product_id | Product sold |
| quantity | Number of units |
| unit_price | Price per unit |
| discount_pct | Line-level discount percentage |

## payments.csv

| Column | Description |
|---|---|
| payment_id | Unique payment identifier |
| order_id | Parent order |
| payment_method | Credit Card, Debit Card, PayPal, Gift Card |
| payment_status | Paid, Failed, Refunded, Pending |
| payment_date | Payment date |
| amount_paid | Amount paid |

## shipments.csv

| Column | Description |
|---|---|
| shipment_id | Unique shipment identifier |
| order_id | Parent order |
| carrier | Shipping carrier |
| ship_date | Date shipped |
| delivery_date | Date delivered |
| shipping_cost | Shipping cost |
| delivery_status | Delivered, In Transit, Delayed, Lost |

## returns.csv

| Column | Description |
|---|---|
| return_id | Unique return identifier |
| order_id | Parent order |
| product_id | Returned product |
| return_date | Return date |
| return_reason | Reason for return |
| refund_amount | Refund amount |
