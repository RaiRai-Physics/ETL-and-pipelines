CREATE TABLE fact_meter_readings (reading_id VARCHAR, meter_id VARCHAR, customer_id VARCHAR, site_id VARCHAR, reading_ts TIMESTAMP, adjusted_kwh DECIMAL(18,4), kw_demand DECIMAL(18,4));
CREATE TABLE fact_generation_hourly (generation_id VARCHAR, site_id VARCHAR, reading_ts TIMESTAMP, generation_mw DECIMAL(18,4), net_generation_mw DECIMAL(18,4), capacity_factor DECIMAL(18,6));
CREATE TABLE dim_customer_scd2 (customer_sk VARCHAR, customer_id VARCHAR, segment VARCHAR, credit_class VARCHAR, effective_start_date DATE, effective_end_date DATE, is_current BOOLEAN);
