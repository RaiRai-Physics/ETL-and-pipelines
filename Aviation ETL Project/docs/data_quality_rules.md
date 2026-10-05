# Data Quality Rules

- Flight schedules must reference valid aircraft and routes.
- Flight events must reference valid scheduled flights.
- Crew assignments must reference valid crew and flights.
- Bookings must reference valid passengers.
- Booking amounts must be numeric.
- Booking segments must reference valid bookings and flights.
- Payments must reference valid bookings and have numeric amounts.
- Refunds must reference valid tickets.
- Maintenance events must reference valid aircraft.
- Maintenance labor hours must be numeric.
- Invalid records are written to Quarantine with an explicit reason.
