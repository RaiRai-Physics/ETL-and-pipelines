# Architecture

## Bronze
Bronze preserves source records and appends ingestion metadata and source-file lineage.

## Silver
Silver applies schema normalization, timestamp/date parsing, numeric casting, deduplication, status standardization, and relational validation.

## Quarantine
Records that violate key data-quality rules are retained with explicit failure reasons rather than silently dropped.

## Gold
Gold contains dimensional history, atomic facts, analytical marts, reconciliations, and operational exception tables.

### Dimensions
- dim_loyalty_scd2
- dim_aircraft_scd2
- dim_airport
- dim_passenger
- dim_crew
- dim_route
- dim_fare_class

### Facts
- fact_flights
- fact_booking_segments
- fact_baggage
- fact_maintenance
- fact_booking_revenue

### Marts
- mart_flight_performance
- mart_route_performance
- mart_baggage_performance
- mart_aircraft_reliability
- mart_crew_workload
- mart_passenger_360
- mart_loyalty_performance
- mart_airport_operations
- mart_refund_risk
