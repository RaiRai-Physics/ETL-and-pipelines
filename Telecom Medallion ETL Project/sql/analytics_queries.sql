-- Network hot spots
SELECT cell_id, site_id, avg_prb_utilization_pct, avg_drop_call_rate_pct, avg_availability_pct FROM mart_cell_network_health ORDER BY high_util_hours DESC;

-- High churn risk
SELECT customer_id, customer_name, ticket_count, dropped_calls, poor_qoe_sessions, past_due_invoices, churn_risk_score FROM mart_customer_360_churn_risk ORDER BY churn_risk_score DESC;

-- Revenue
SELECT billing_month, invoiced_amount, collected_amount, open_amount, collection_rate FROM mart_monthly_revenue ORDER BY billing_month;
