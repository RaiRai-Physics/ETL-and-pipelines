# Data Quality Rules

- Business keys must be unique after cleaning.
- Encounters require valid patients and providers.
- Encounter discharge cannot precede admission.
- Diagnoses and procedures require valid encounters and codes.
- Lab orders require valid encounters/tests; lab results require a valid lab order and numeric value.
- Medication orders require valid medications; administrations require valid orders.
- Claims require valid encounters/payers and numeric charges.
- Claim lines require valid claims/procedure codes.
- Claim payments require valid claims.
- Patient surveys require valid encounters.
- Invalid records are retained in `data/quarantine/python/` with a reason.
