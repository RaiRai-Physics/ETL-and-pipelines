-- Lowest OEE lines
SELECT line_id, AVG(oee) avg_oee FROM mart_daily_line_oee GROUP BY line_id ORDER BY avg_oee;

-- Weakest machine reliability
SELECT * FROM mart_machine_reliability ORDER BY mtbf_hours;

-- Supplier risk
SELECT * FROM mart_supplier_scorecard ORDER BY fill_rate, reject_rate DESC;

-- Stockout risk
SELECT * FROM mart_inventory_risk WHERE stockout_flag = TRUE OR below_safety_stock_flag = TRUE;

-- Work order variance
SELECT * FROM recon_work_orders_vs_production ORDER BY completion_pct;
