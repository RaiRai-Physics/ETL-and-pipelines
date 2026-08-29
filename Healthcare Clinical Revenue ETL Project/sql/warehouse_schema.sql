CREATE TABLE dim_patient_scd2 (
  patient_sk VARCHAR,
  patient_id VARCHAR,
  date_of_birth DATE,
  sex_at_birth VARCHAR,
  primary_payer_id VARCHAR,
  effective_start_date DATE,
  effective_end_date DATE,
  is_current BOOLEAN
);

CREATE TABLE fact_encounters (
  encounter_id VARCHAR,
  patient_id VARCHAR,
  provider_id VARCHAR,
  facility_id VARCHAR,
  department_id VARCHAR,
  encounter_type VARCHAR,
  admit_ts TIMESTAMP,
  discharge_ts TIMESTAMP,
  length_of_stay_days DECIMAL(18,4),
  age_at_encounter INTEGER,
  high_los_flag BOOLEAN
);

CREATE TABLE fact_claims (
  claim_id VARCHAR,
  encounter_id VARCHAR,
  payer_id VARCHAR,
  total_charge DECIMAL(18,2),
  allowed_amount DECIMAL(18,2),
  patient_responsibility DECIMAL(18,2),
  total_payment DECIMAL(18,2),
  payment_difference DECIMAL(18,2),
  reconciliation_status VARCHAR
);
