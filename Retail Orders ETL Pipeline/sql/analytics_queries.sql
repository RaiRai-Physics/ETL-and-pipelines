-- Sample Analytics queries for the output layer.

-- 1. Top categories by revenue
SELECT
    category,
    subcategory,
    units_sold,
    net_sales,
    gross_margin
FROM category_revenue_summary
ORDER BY net_sales DESC;

-- 2. Best customers by lifetime value
SELECT
    customer_id,
    full_name,
    city,
    state,
    loyalty_tier,
    order_count,
    lifetime_net_sales
FROM customer_lifetime_value
ORDER BY lifetime_net_sales DESC;

-- 3. Late shipment count by carrier
SELECT
    carrier,
    COUNT(*) AS late_shipment_count
FROM late_shipment_report
GROUP BY carrier
ORDER BY late_shipment_count DESC;

-- 4. Products with highest return rate
SELECT
    product_id,
    product_name,
    category,
    units_sold,
    return_count,
    return_rate
FROM return_rate_by_product
ORDER BY return_rate DESC;

-- 5. Payment mismatches
SELECT
    order_id,
    customer_id,
    order_status,
    order_net_sales,
    total_paid,
    payment_difference,
    reconciliation_status
FROM payment_reconciliation_report
WHERE reconciliation_status = 'Mismatch';
