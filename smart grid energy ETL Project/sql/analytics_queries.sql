SELECT region, SUM(consumption_kwh) FROM mart_daily_consumption GROUP BY region;
SELECT asset_id, anomaly_count, anomaly_rate FROM mart_asset_health ORDER BY anomaly_rate DESC;
SELECT region, outage_count, saidi_proxy, saifi_proxy FROM mart_grid_reliability;
SELECT site_id, model_version, mae_mw, mape FROM mart_forecast_accuracy ORDER BY mape DESC;
