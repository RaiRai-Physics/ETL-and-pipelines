CREATE TABLE dim_customer_scd2 (
  customer_sk VARCHAR,
  customer_id VARCHAR,
  customer_name VARCHAR,
  loyalty_tier VARCHAR,
  effective_start_date DATE,
  effective_end_date DATE,
  is_current BOOLEAN
);

CREATE TABLE fact_pos_sales_lines (
  transaction_line_id VARCHAR,
  transaction_id VARCHAR,
  store_id VARCHAR,
  customer_id VARCHAR,
  product_id VARCHAR,
  transaction_ts TIMESTAMP,
  quantity DECIMAL(18,2),
  unit_price DECIMAL(18,2),
  net_sales DECIMAL(18,2),
  estimated_cogs DECIMAL(18,2),
  gross_margin DECIMAL(18,2)
);
