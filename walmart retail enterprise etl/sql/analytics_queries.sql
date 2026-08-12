-- Top stores
SELECT store_id, store_name, total_sales, digital_mix
FROM mart_omnichannel_store_performance
ORDER BY total_sales DESC;

-- Stockout hotspots
SELECT store_id, store_name, stockout_sku_count, low_stock_sku_count
FROM mart_store_inventory_health
ORDER BY stockout_sku_count DESC;

-- Product return risk
SELECT product_id, department, net_sales, return_rate
FROM mart_product_performance
ORDER BY return_rate DESC;

-- Payment reconciliation exceptions
SELECT transaction_id, basket_sales, payment_amount, difference
FROM recon_pos_sales_vs_payments
WHERE reconciliation_status = 'Mismatch';

-- Best promotions
SELECT promotion_id, promotion_name, transaction_count, units, promo_sales, markdown_amount
FROM mart_promotion_performance
ORDER BY promo_sales DESC;
