-- 30-day readmissions
SELECT facility_id, department_id, COUNT(*) AS readmissions_30d
FROM mart_readmissions_30d
WHERE readmission_30d_flag = TRUE
GROUP BY facility_id, department_id
ORDER BY readmissions_30d DESC;

-- Payer denial performance
SELECT payer_name, claim_count, denied_claims, denial_rate, collection_rate
FROM mart_payer_performance
ORDER BY denial_rate DESC;

-- High bed occupancy departments
SELECT facility_name, department_name, avg_occupancy_pct, high_occupancy_days
FROM mart_bed_utilization
ORDER BY high_occupancy_days DESC;

-- Lab turnaround
SELECT lab_test_name, result_count, avg_turnaround_hours, p95_turnaround_hours, turnaround_alert_count
FROM mart_lab_turnaround
ORDER BY p95_turnaround_hours DESC;

-- Claim reconciliation exceptions
SELECT claim_id, payer_id, expected_collection, total_payment, payment_difference
FROM exception_claim_payment_mismatch
ORDER BY ABS(payment_difference) DESC;
