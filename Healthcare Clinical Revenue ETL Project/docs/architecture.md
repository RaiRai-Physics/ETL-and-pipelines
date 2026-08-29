# Architecture

## Source systems simulated

- EHR core: encounters, diagnoses, procedures
- Ambulatory scheduling: appointments
- LIS: lab orders and results
- MAR/pharmacy: medication orders and administrations
- Revenue cycle: claims, claim lines, payments
- Hospital operations: beds and patient experience
- MDM/reference: patients, providers, facilities, codes, payers
- CDC: patient payer/status and provider specialty/status changes

## Gold model

Dimensions: patient SCD2, provider SCD2, facility, department, payer, diagnosis, procedure, medication, lab test.

Facts: encounters, encounter diagnoses, procedures, lab results, medication administrations, claims, claim lines, beds, appointments, patient satisfaction.

Marts: readmissions, disease burden, lab turnaround, medication administration, payer performance, revenue cycle, bed utilization, appointment access, experience, provider performance, department quality, patient 360.
