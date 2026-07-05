# Architecture Notes

Source systems: ERP, OMS, WMS, TMS, MDM, and reference feeds.

Warehouse model: SCD Type 2 dimensions, transaction facts, operational marts, reconciliation reports, and exception reports.

Incremental watermarks are stored in `metadata/run_watermarks.json`.
