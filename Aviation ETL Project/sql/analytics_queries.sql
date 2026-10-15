-- Route performance
SELECT route_id, origin_airport, destination_airport, flight_count, on_time_rate, cancellation_rate, passengers, revenue, avg_load_factor
FROM mart_route_performance
ORDER BY revenue DESC;

-- Aircraft reliability
SELECT aircraft_id, tail_number, model_code, flight_count, maintenance_events, maintenance_hours, maintenance_cost, aog_events, mx_events_per_100_flights
FROM mart_aircraft_reliability
ORDER BY aog_events DESC;

-- Airport delays
SELECT origin_airport, airport_name, departures, avg_delay, long_delay_rate, cancelled, passengers
FROM mart_airport_operations
ORDER BY avg_delay DESC;

-- Passenger value
SELECT passenger_id, passenger_name, loyalty_tier, flights_booked, flown_segments, segment_revenue, no_show_rate, value_tier
FROM mart_passenger_360
ORDER BY segment_revenue DESC;

-- Payment mismatches
SELECT booking_id, total_booking_amount, captured_amount, difference
FROM recon_bookings_vs_payments
WHERE reconciliation_status='Mismatch';
