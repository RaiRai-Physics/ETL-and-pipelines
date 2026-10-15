CREATE TABLE dim_loyalty_scd2 (
  loyalty_sk VARCHAR,
  loyalty_id VARCHAR,
  member_name VARCHAR,
  loyalty_tier VARCHAR,
  effective_start_date DATE,
  effective_end_date DATE,
  is_current BOOLEAN
);

CREATE TABLE fact_flights (
  flight_id VARCHAR,
  route_id VARCHAR,
  aircraft_id VARCHAR,
  flight_date DATE,
  scheduled_departure_ts TIMESTAMP,
  actual_departure_ts TIMESTAMP,
  departure_delay_minutes NUMBER,
  arrival_delay_minutes NUMBER,
  on_time_departure_flag BOOLEAN,
  long_delay_flag BOOLEAN,
  cancelled_flag BOOLEAN
);

CREATE TABLE fact_booking_segments (
  booking_segment_id VARCHAR,
  booking_id VARCHAR,
  passenger_id VARCHAR,
  flight_id VARCHAR,
  fare_class VARCHAR,
  segment_revenue NUMBER(18,2),
  flown_flag BOOLEAN,
  no_show_flag BOOLEAN
);
