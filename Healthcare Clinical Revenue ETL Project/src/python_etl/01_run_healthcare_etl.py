from pathlib import Path
import sys,json
import pandas as pd
import numpy as np

ROOT=Path(__file__).resolve().parents[2]; sys.path.append(str(ROOT))
from src.common.utils import load_config,read_csv,write_csv,clean_text,parse_date,parse_ts,parse_num,parse_bool,stable_hash,safe_div,mask_email,mask_phone

CFG=load_config(); BRONZE=ROOT/CFG['bronze_path']; SILVER=ROOT/CFG['silver_path']; Q=ROOT/CFG['quarantine_path']; GOLD=ROOT/CFG['gold_path']; REPORT=ROOT/CFG['report_path']; META=ROOT/CFG['metadata_path']
RAW={
'facilities':'data/raw/master/facilities.csv','departments':'data/raw/master/departments.csv','providers':'data/raw/master/providers.csv','payers':'data/raw/master/payers.csv',
'diagnosis_codes':'data/raw/master/diagnosis_codes.csv','procedure_codes':'data/raw/master/procedure_codes.csv','medications':'data/raw/master/medications.csv','lab_test_reference':'data/raw/master/lab_test_reference.csv','patients':'data/raw/master/patients.csv','calendar':'data/raw/reference/calendar.csv',
'appointments':'data/raw/clinical/appointments.csv','encounters':'data/raw/clinical/encounters.csv','encounter_diagnoses':'data/raw/clinical/encounter_diagnoses.csv','encounter_procedures':'data/raw/clinical/encounter_procedures.csv','lab_orders':'data/raw/clinical/lab_orders.csv','lab_results':'data/raw/clinical/lab_results.csv','med_orders':'data/raw/clinical/med_orders.csv','med_admin':'data/raw/clinical/med_admin.csv',
'claims':'data/raw/revenue/claims.csv','claim_lines':'data/raw/revenue/claim_lines.csv','claim_payments':'data/raw/revenue/claim_payments.csv','bed_occupancy':'data/raw/operations/bed_occupancy.csv','patient_satisfaction':'data/raw/operations/patient_satisfaction.csv','patient_cdc':'data/raw/cdc/patient_cdc.csv','provider_cdc':'data/raw/cdc/provider_cdc.csv'}

def load(name): return read_csv(ROOT/RAW[name])
def save_bronze(name,df):
    x=df.copy(); x['_loaded_at_utc']=pd.Timestamp.utcnow().isoformat(); x['_source_file']=RAW[name]; write_csv(x,BRONZE/f'{name}.csv')
def save_silver(name,df): write_csv(df,SILVER/f'{name}.csv')
def save_gold(name,df): write_csv(df,GOLD/f'{name}.csv')
def save_q(name,df,reason):
    x=df.copy(); x['quarantine_reason']=reason
    if len(x)==0: x=pd.DataFrame(columns=['quarantine_reason'])
    write_csv(x,Q/f'{name}.csv')

# ---------- cleaners ----------
def c_facilities(x):
    x=x.copy(); x['facility_id']=clean_text(x['facility_id'],'','upper'); x['facility_name']=clean_text(x['facility_name'],case='title'); x['facility_type']=clean_text(x['facility_type'],case='title'); x['city']=clean_text(x['city'],case='title'); x['state']=clean_text(x['state'],case='upper'); x['region']=clean_text(x['region'],case='title'); x['licensed_beds']=parse_num(x['licensed_beds'],0); x['open_date']=parse_date(x['open_date']); x['active_flag']=parse_bool(x['active_flag']); return x.drop_duplicates('facility_id')
def c_departments(x):
    x=x.copy(); x['department_id']=clean_text(x['department_id'],'','upper'); x['facility_id']=clean_text(x['facility_id'],'','upper'); x['department_name']=clean_text(x['department_name'],case='title'); x['service_line']=clean_text(x['service_line'],case='title'); x['active_flag']=parse_bool(x['active_flag']); return x.drop_duplicates('department_id')
def c_providers(x):
    x=x.copy(); x['provider_id']=clean_text(x['provider_id'],'','upper'); x['provider_name']=clean_text(x['provider_name'],case='title'); x['facility_id']=clean_text(x['facility_id'],'','upper'); x['department_id']=clean_text(x['department_id'],'','upper'); x['specialty']=clean_text(x['specialty'],case='title'); x['hire_date']=parse_date(x['hire_date']); x['employment_status']=clean_text(x['employment_status'],case='title'); return x.drop_duplicates('provider_id')
def c_payers(x):
    x=x.copy(); x['payer_id']=clean_text(x['payer_id'],'','upper'); x['payer_name']=clean_text(x['payer_name'],case='title'); x['payer_type']=clean_text(x['payer_type'],case='title'); return x.drop_duplicates('payer_id')
def c_diag(x):
    x=x.copy(); x['diagnosis_code']=clean_text(x['diagnosis_code'],'','upper'); x['diagnosis_description']=clean_text(x['diagnosis_description'],case='title'); x['diagnosis_group']=clean_text(x['diagnosis_group'],case='title'); return x.drop_duplicates('diagnosis_code')
def c_proc(x):
    x=x.copy(); x['procedure_code']=clean_text(x['procedure_code'],'','upper'); x['procedure_description']=clean_text(x['procedure_description'],case='title'); x['procedure_group']=clean_text(x['procedure_group'],case='title'); x['standard_charge']=parse_num(x['standard_charge']); return x.drop_duplicates('procedure_code')
def c_meds(x):
    x=x.copy(); x['medication_id']=clean_text(x['medication_id'],'','upper'); x['medication_name']=clean_text(x['medication_name'],case='title'); x['medication_class']=clean_text(x['medication_class'],case='title'); x['active_flag']=parse_bool(x['active_flag']); return x.drop_duplicates('medication_id')
def c_labref(x):
    x=x.copy(); x['lab_test_id']=clean_text(x['lab_test_id'],'','upper'); x['lab_test_name']=clean_text(x['lab_test_name'],case='title'); x['ref_low']=parse_num(x['ref_low']); x['ref_high']=parse_num(x['ref_high']); return x.drop_duplicates('lab_test_id')
def c_patients(x):
    x=x.copy(); x['patient_id']=clean_text(x['patient_id'],'','upper'); x['full_name']=clean_text(x['full_name'],case='title'); x['date_of_birth']=parse_date(x['date_of_birth']); x['sex_at_birth']=clean_text(x['sex_at_birth'],case='upper').replace({'F':'FEMALE','M':'MALE'}); x['email_masked']=x['email'].map(mask_email); x['phone_masked']=x['phone'].map(mask_phone); x['city']=clean_text(x['city'],case='title'); x['state']=clean_text(x['state'],case='upper'); x['primary_payer_id']=clean_text(x['primary_payer_id'],'','upper'); x['registration_date']=parse_date(x['registration_date']); x['active_flag']=parse_bool(x['active_flag']); x=x.drop(columns=['email','phone']); x['patient_hash']=x['patient_id'].map(stable_hash); return x.drop_duplicates('patient_id')
def c_calendar(x):
    x=x.copy(); x['calendar_date']=parse_date(x['calendar_date']); return x.drop_duplicates('date_key')
def c_appts(x):
    x=x.copy();
    for c in ['appointment_id','patient_id','provider_id','facility_id','department_id']: x[c]=clean_text(x[c],'','upper')
    x['scheduled_ts']=parse_ts(x['scheduled_ts']); x['scheduled_date']=x['scheduled_ts'].dt.date.astype('string'); x['appointment_type']=clean_text(x['appointment_type'],case='title'); x['appointment_status']=clean_text(x['appointment_status'],case='title'); x['booking_channel']=clean_text(x['booking_channel'],case='title'); return x.drop_duplicates('appointment_id')
def c_encounters(x):
    x=x.copy();
    for c in ['encounter_id','patient_id','provider_id','facility_id','department_id']: x[c]=clean_text(x[c],'','upper')
    x['encounter_type']=clean_text(x['encounter_type'],case='title'); x['admit_ts']=parse_ts(x['admit_ts']); x['discharge_ts']=parse_ts(x['discharge_ts']); x['admit_date']=x['admit_ts'].dt.date.astype('string'); x['discharge_date']=x['discharge_ts'].dt.date.astype('string'); x['disposition']=clean_text(x['disposition'],case='title'); x['acuity_level']=parse_num(x['acuity_level']); x['source_system']=clean_text(x['source_system'],case='lower'); x['length_of_stay_days']=(x['discharge_ts']-x['admit_ts']).dt.total_seconds()/86400; return x.drop_duplicates('encounter_id')
def c_edx(x):
    x=x.copy(); x['encounter_diagnosis_id']=clean_text(x['encounter_diagnosis_id'],'','upper'); x['encounter_id']=clean_text(x['encounter_id'],'','upper'); x['diagnosis_code']=clean_text(x['diagnosis_code'],'','upper'); x['diagnosis_type']=clean_text(x['diagnosis_type'],case='title'); x['present_on_admission']=clean_text(x['present_on_admission'],case='upper'); return x.drop_duplicates('encounter_diagnosis_id')
def c_eproc(x):
    x=x.copy(); x['encounter_procedure_id']=clean_text(x['encounter_procedure_id'],'','upper'); x['encounter_id']=clean_text(x['encounter_id'],'','upper'); x['procedure_code']=clean_text(x['procedure_code'],'','upper'); x['procedure_ts']=parse_ts(x['procedure_ts']); x['performing_provider_id']=clean_text(x['performing_provider_id'],'','upper'); x['quantity']=parse_num(x['quantity']); return x.drop_duplicates('encounter_procedure_id')
def c_lab_orders(x):
    x=x.copy();
    for c in ['lab_order_id','encounter_id','patient_id','lab_test_id','ordering_provider_id']: x[c]=clean_text(x[c],'','upper')
    x['order_ts']=parse_ts(x['order_ts']); x['order_status']=clean_text(x['order_status'],case='title'); return x.drop_duplicates('lab_order_id')
def c_lab_results(x):
    x=x.copy(); x['lab_result_id']=clean_text(x['lab_result_id'],'','upper'); x['lab_order_id']=clean_text(x['lab_order_id'],'','upper'); x['result_ts']=parse_ts(x['result_ts']); x['result_value']=parse_num(x['result_value']); x['abnormal_flag']=clean_text(x['abnormal_flag'],case='upper'); x['result_status']=clean_text(x['result_status'],case='title'); return x.drop_duplicates('lab_result_id')
def c_med_orders(x):
    x=x.copy();
    for c in ['med_order_id','encounter_id','patient_id','medication_id']: x[c]=clean_text(x[c],'','upper')
    x['order_ts']=parse_ts(x['order_ts']); x['dose']=parse_num(x['dose']); x['route']=clean_text(x['route'],case='upper'); x['order_status']=clean_text(x['order_status'],case='title'); return x.drop_duplicates('med_order_id')
def c_med_admin(x):
    x=x.copy(); x['med_admin_id']=clean_text(x['med_admin_id'],'','upper'); x['med_order_id']=clean_text(x['med_order_id'],'','upper'); x['admin_ts']=parse_ts(x['admin_ts']); x['administered_dose']=parse_num(x['administered_dose']); x['admin_status']=clean_text(x['admin_status'],case='title'); return x.drop_duplicates('med_admin_id')
def c_claims(x):
    x=x.copy();
    for c in ['claim_id','encounter_id','patient_id','payer_id']: x[c]=clean_text(x[c],'','upper')
    x['claim_submission_date']=parse_date(x['claim_submission_date']); x['claim_status']=clean_text(x['claim_status'],case='title'); x['total_charge']=parse_num(x['total_charge']); x['allowed_amount']=parse_num(x['allowed_amount'],0); x['patient_responsibility']=parse_num(x['patient_responsibility'],0); x['denial_reason']=clean_text(x['denial_reason'],''); return x.drop_duplicates('claim_id')
def c_claim_lines(x):
    x=x.copy(); x['claim_line_id']=clean_text(x['claim_line_id'],'','upper'); x['claim_id']=clean_text(x['claim_id'],'','upper'); x['procedure_code']=clean_text(x['procedure_code'],'','upper'); x['units']=parse_num(x['units']); x['charge_amount']=parse_num(x['charge_amount']); x['allowed_amount']=parse_num(x['allowed_amount'],0); x['line_status']=clean_text(x['line_status'],case='title'); return x.drop_duplicates('claim_line_id')
def c_claim_pay(x):
    x=x.copy(); x['claim_payment_id']=clean_text(x['claim_payment_id'],'','upper'); x['claim_id']=clean_text(x['claim_id'],'','upper'); x['payment_date']=parse_date(x['payment_date']); x['payer_payment_amount']=parse_num(x['payer_payment_amount'],0); x['patient_payment_amount']=parse_num(x['patient_payment_amount'],0); x['payment_method']=clean_text(x['payment_method'],case='upper'); x['payment_status']=clean_text(x['payment_status'],case='title'); return x.drop_duplicates('claim_payment_id')
def c_beds(x):
    x=x.copy(); x['snapshot_date']=parse_date(x['snapshot_date']); x['facility_id']=clean_text(x['facility_id'],'','upper'); x['department_id']=clean_text(x['department_id'],'','upper');
    for c in ['staffed_beds','occupied_beds','blocked_beds','boarding_patients']: x[c]=parse_num(x[c],0)
    x['available_staffed_beds']=(x['staffed_beds']-x['blocked_beds']).clip(lower=0); x['occupancy_pct']=safe_div(x['occupied_beds'],x['available_staffed_beds']); return x.drop_duplicates(['snapshot_date','facility_id','department_id'],keep='last')
def c_satisfaction(x):
    x=x.copy(); x['survey_id']=clean_text(x['survey_id'],'','upper'); x['encounter_id']=clean_text(x['encounter_id'],'','upper'); x['survey_date']=parse_date(x['survey_date']);
    for c in ['overall_rating','communication_rating','cleanliness_rating']: x[c]=parse_num(x[c]); x['would_recommend']=parse_bool(x['would_recommend'])
    return x.drop_duplicates('survey_id')
def c_patient_cdc(x):
    x=x.copy(); x['cdc_id']=clean_text(x['cdc_id'],'','upper'); x['patient_id']=clean_text(x['patient_id'],'','upper'); x['effective_ts']=parse_ts(x['effective_ts']); x['effective_date']=x['effective_ts'].dt.date.astype('string'); x['operation']=clean_text(x['operation'],case='upper'); x['new_primary_payer_id']=clean_text(x['new_primary_payer_id'],'','upper'); x['new_active_flag']=parse_bool(x['new_active_flag']); return x.drop_duplicates('cdc_id')
def c_provider_cdc(x):
    x=x.copy(); x['cdc_id']=clean_text(x['cdc_id'],'','upper'); x['provider_id']=clean_text(x['provider_id'],'','upper'); x['effective_ts']=parse_ts(x['effective_ts']); x['effective_date']=x['effective_ts'].dt.date.astype('string'); x['operation']=clean_text(x['operation'],case='upper'); x['new_specialty']=clean_text(x['new_specialty'],case='title'); x['new_employment_status']=clean_text(x['new_employment_status'],case='title'); return x.drop_duplicates('cdc_id')

CLEAN={'facilities':c_facilities,'departments':c_departments,'providers':c_providers,'payers':c_payers,'diagnosis_codes':c_diag,'procedure_codes':c_proc,'medications':c_meds,'lab_test_reference':c_labref,'patients':c_patients,'calendar':c_calendar,'appointments':c_appts,'encounters':c_encounters,'encounter_diagnoses':c_edx,'encounter_procedures':c_eproc,'lab_orders':c_lab_orders,'lab_results':c_lab_results,'med_orders':c_med_orders,'med_admin':c_med_admin,'claims':c_claims,'claim_lines':c_claim_lines,'claim_payments':c_claim_pay,'bed_occupancy':c_beds,'patient_satisfaction':c_satisfaction,'patient_cdc':c_patient_cdc,'provider_cdc':c_provider_cdc}

# ---------- SCD2 ----------
def patient_scd2(patients,cdc):
    base=patients[['patient_id','full_name','date_of_birth','sex_at_birth','email_masked','phone_masked','city','state','zip3','primary_payer_id','registration_date','active_flag']].copy(); base['effective_start_date']=base['registration_date'].fillna('1900-01-01'); base['record_source']='patient_master'
    upd=cdc.merge(patients[['patient_id','full_name','date_of_birth','sex_at_birth','email_masked','phone_masked','city','state','zip3','registration_date']],on='patient_id',how='inner').rename(columns={'new_primary_payer_id':'primary_payer_id','new_active_flag':'active_flag','effective_date':'effective_start_date','change_reason':'record_source'}); upd=upd[base.columns]
    x=pd.concat([base,upd],ignore_index=True); x['_d']=pd.to_datetime(x['effective_start_date'],errors='coerce'); x=x.sort_values(['patient_id','_d']); x['effective_end_date']=x.groupby('patient_id')['_d'].shift(-1).dt.date.astype('string').fillna('9999-12-31'); x['is_current']=x['effective_end_date'].eq('9999-12-31'); x['patient_sk']=[stable_hash(a,b,c) for a,b,c in zip(x['patient_id'],x['effective_start_date'],x['primary_payer_id'])]; return x.drop(columns='_d')
def provider_scd2(providers,cdc):
    base=providers[['provider_id','provider_name','facility_id','department_id','specialty','npi_like_id','hire_date','employment_status']].copy(); base['effective_start_date']=base['hire_date'].fillna('1900-01-01'); base['record_source']='provider_master'
    upd=cdc.merge(providers[['provider_id','provider_name','facility_id','department_id','npi_like_id','hire_date']],on='provider_id',how='inner').rename(columns={'new_specialty':'specialty','new_employment_status':'employment_status','effective_date':'effective_start_date','change_reason':'record_source'}); upd=upd[base.columns]
    x=pd.concat([base,upd],ignore_index=True); x['_d']=pd.to_datetime(x['effective_start_date'],errors='coerce'); x=x.sort_values(['provider_id','_d']); x['effective_end_date']=x.groupby('provider_id')['_d'].shift(-1).dt.date.astype('string').fillna('9999-12-31'); x['is_current']=x['effective_end_date'].eq('9999-12-31'); x['provider_sk']=[stable_hash(a,b,c) for a,b,c in zip(x['provider_id'],x['effective_start_date'],x['specialty'])]; return x.drop(columns='_d')

def main():
    for d in [BRONZE,SILVER,Q,GOLD,REPORT,META]: d.mkdir(parents=True,exist_ok=True)
    data={}; raw_counts={}
    for name in RAW:
        r=load(name); raw_counts[name]=len(r); save_bronze(name,r); data[name]=CLEAN[name](r); save_silver(name,data[name])

    fac,dep,prv,payers,diag,proc,meds,labref,patients=data['facilities'],data['departments'],data['providers'],data['payers'],data['diagnosis_codes'],data['procedure_codes'],data['medications'],data['lab_test_reference'],data['patients']
    appts,enc,edx,eproc,lorders,lresults,morders,madmin,claims,clines,cpay,beds,sat=data['appointments'],data['encounters'],data['encounter_diagnoses'],data['encounter_procedures'],data['lab_orders'],data['lab_results'],data['med_orders'],data['med_admin'],data['claims'],data['claim_lines'],data['claim_payments'],data['bed_occupancy'],data['patient_satisfaction']

    qcounts={}
    def q(name,df,reason): save_q(name,df,reason); qcounts[name]=len(df)
    q('patients_invalid_payer',patients[~patients['primary_payer_id'].isin(payers['payer_id'])],'Patient references missing payer')
    q('providers_invalid_facility',prv[~prv['facility_id'].isin(fac['facility_id'])],'Provider references missing facility')
    q('providers_invalid_department',prv[~prv['department_id'].isin(dep['department_id'])],'Provider references missing department')
    q('appointments_invalid_patient',appts[~appts['patient_id'].isin(patients['patient_id'])],'Appointment references missing patient')
    q('appointments_invalid_provider',appts[~appts['provider_id'].isin(prv['provider_id'])],'Appointment references missing provider')
    q('encounters_invalid_patient',enc[~enc['patient_id'].isin(patients['patient_id'])],'Encounter references missing patient')
    q('encounters_invalid_provider',enc[~enc['provider_id'].isin(prv['provider_id'])],'Encounter references missing provider')
    q('encounters_bad_timestamps',enc[enc['admit_ts'].isna() | (enc['discharge_ts'].notna() & (enc['discharge_ts']<enc['admit_ts']))],'Invalid encounter timestamps')
    q('diagnoses_invalid_encounter',edx[~edx['encounter_id'].isin(enc['encounter_id'])],'Diagnosis references missing encounter')
    q('diagnoses_invalid_code',edx[~edx['diagnosis_code'].isin(diag['diagnosis_code'])],'Unknown diagnosis code')
    q('procedures_invalid_encounter',eproc[~eproc['encounter_id'].isin(enc['encounter_id'])],'Procedure references missing encounter')
    q('procedures_invalid_code',eproc[~eproc['procedure_code'].isin(proc['procedure_code'])],'Unknown procedure code')
    q('lab_orders_invalid_encounter',lorders[~lorders['encounter_id'].isin(enc['encounter_id'])],'Lab order references missing encounter')
    q('lab_orders_invalid_test',lorders[~lorders['lab_test_id'].isin(labref['lab_test_id'])],'Unknown lab test')
    q('lab_results_invalid_order',lresults[~lresults['lab_order_id'].isin(lorders['lab_order_id'])],'Lab result references missing order')
    q('lab_results_bad_value',lresults[lresults['result_value'].isna()],'Non-numeric lab result')
    q('med_orders_invalid_medication',morders[~morders['medication_id'].isin(meds['medication_id'])],'Unknown medication')
    q('med_admin_invalid_order',madmin[~madmin['med_order_id'].isin(morders['med_order_id'])],'Medication administration references missing order')
    q('claims_invalid_encounter',claims[~claims['encounter_id'].isin(enc['encounter_id'])],'Claim references missing encounter')
    q('claims_invalid_payer',claims[~claims['payer_id'].isin(payers['payer_id'])],'Claim references missing payer')
    q('claims_bad_charge',claims[claims['total_charge'].isna()],'Non-numeric claim total charge')
    q('claim_lines_invalid_claim',clines[~clines['claim_id'].isin(claims['claim_id'])],'Claim line references missing claim')
    q('claim_lines_invalid_procedure',clines[~clines['procedure_code'].isin(proc['procedure_code'])],'Unknown claim procedure code')
    q('claim_payments_invalid_claim',cpay[~cpay['claim_id'].isin(claims['claim_id'])],'Claim payment references missing claim')
    q('satisfaction_invalid_encounter',sat[~sat['encounter_id'].isin(enc['encounter_id'])],'Survey references missing encounter')

    pscd=patient_scd2(patients,data['patient_cdc']); vscd=provider_scd2(prv,data['provider_cdc'])
    save_gold('dim_patient_scd2',pscd); save_gold('dim_patient_current',pscd[pscd['is_current']]); save_gold('dim_provider_scd2',vscd); save_gold('dim_provider_current',vscd[vscd['is_current']])
    for n,d in {'dim_facility':fac,'dim_department':dep,'dim_payer':payers,'dim_diagnosis':diag,'dim_procedure':proc,'dim_medication':meds,'dim_lab_test':labref}.items(): save_gold(n,d)

    # trusted encounter fact
    vf=enc[enc['patient_id'].isin(patients['patient_id']) & enc['provider_id'].isin(prv['provider_id']) & enc['admit_ts'].notna()].copy()
    vf=vf.merge(patients[['patient_id','date_of_birth','primary_payer_id']],on='patient_id',how='left'); vf['age_at_encounter']=((vf['admit_ts']-pd.to_datetime(vf['date_of_birth'],errors='coerce')).dt.days/365.25).fillna(0).astype(int); vf['high_los_flag']=vf['length_of_stay_days']>CFG['high_los_days']; vf['inpatient_flag']=vf['encounter_type'].eq('Inpatient'); save_gold('fact_encounters',vf)

    # readmissions
    inpatient=vf[(vf['encounter_type']=='Inpatient') & vf['discharge_ts'].notna()].sort_values(['patient_id','admit_ts']).copy(); inpatient['previous_discharge_ts']=inpatient.groupby('patient_id')['discharge_ts'].shift(1); inpatient['days_since_previous_discharge']=(inpatient['admit_ts']-inpatient['previous_discharge_ts']).dt.total_seconds()/86400; inpatient['readmission_30d_flag']=inpatient['days_since_previous_discharge'].between(0,CFG['readmission_window_days'],inclusive='both'); save_gold('mart_readmissions_30d',inpatient)

    # diagnoses burden
    vdx=edx[edx['encounter_id'].isin(vf['encounter_id']) & edx['diagnosis_code'].isin(diag['diagnosis_code'])].merge(diag,on='diagnosis_code',how='left').merge(vf[['encounter_id','patient_id','facility_id','encounter_type']],on='encounter_id',how='left'); save_gold('fact_encounter_diagnoses',vdx)
    disease=vdx.groupby(['diagnosis_code','diagnosis_description','diagnosis_group'],as_index=False).agg(encounter_count=('encounter_id','nunique'),patient_count=('patient_id','nunique')); save_gold('mart_disease_burden',disease.sort_values('encounter_count',ascending=False))

    # procedures
    vproc=eproc[eproc['encounter_id'].isin(vf['encounter_id']) & eproc['procedure_code'].isin(proc['procedure_code']) & eproc['quantity'].notna()].merge(proc,on='procedure_code',how='left'); save_gold('fact_encounter_procedures',vproc)

    # labs
    vlabs=lresults[lresults['lab_order_id'].isin(lorders['lab_order_id']) & lresults['result_value'].notna()].merge(lorders,on='lab_order_id',how='inner').merge(labref,on='lab_test_id',how='left'); vlabs['turnaround_hours']=(vlabs['result_ts']-vlabs['order_ts']).dt.total_seconds()/3600; vlabs['turnaround_alert_flag']=vlabs['turnaround_hours']>CFG['lab_turnaround_alert_hours']; vlabs['computed_abnormal_flag']=np.where(vlabs['result_value']>vlabs['ref_high'],'H',np.where(vlabs['result_value']<vlabs['ref_low'],'L','N')); save_gold('fact_lab_results',vlabs)
    labmart=vlabs.groupby(['lab_test_id','lab_test_name'],as_index=False).agg(result_count=('lab_result_id','count'),avg_turnaround_hours=('turnaround_hours','mean'),p95_turnaround_hours=('turnaround_hours',lambda x:x.quantile(.95)),abnormal_result_count=('computed_abnormal_flag',lambda x:(x!='N').sum()),turnaround_alert_count=('turnaround_alert_flag','sum')); save_gold('mart_lab_turnaround',labmart)

    # medication administrations
    vmadmin=madmin[madmin['med_order_id'].isin(morders['med_order_id'])].merge(morders,on='med_order_id',how='inner').merge(meds,on='medication_id',how='left'); vmadmin['admin_delay_hours']=(vmadmin['admin_ts']-vmadmin['order_ts']).dt.total_seconds()/3600; vmadmin['given_flag']=vmadmin['admin_status'].eq('Given'); save_gold('fact_medication_administration',vmadmin)
    medmart=vmadmin.groupby(['medication_id','medication_name','medication_class'],as_index=False).agg(administration_count=('med_admin_id','count'),given_count=('given_flag','sum'),avg_admin_delay_hours=('admin_delay_hours','mean')); medmart['given_rate']=safe_div(medmart['given_count'],medmart['administration_count']); save_gold('mart_medication_administration',medmart)

    # claims + reconciliation
    vclaims=claims[claims['encounter_id'].isin(vf['encounter_id']) & claims['payer_id'].isin(payers['payer_id']) & claims['total_charge'].notna()].merge(payers,on='payer_id',how='left'); posted=cpay[(cpay['claim_id'].isin(vclaims['claim_id'])) & (cpay['payment_status']=='Posted')].copy(); posted['total_payment']=posted['payer_payment_amount']+posted['patient_payment_amount']; paysum=posted.groupby('claim_id',as_index=False).agg(total_payment=('total_payment','sum'),payer_payment=('payer_payment_amount','sum'),patient_payment=('patient_payment_amount','sum'),payment_count=('claim_payment_id','count'),last_payment_date=('payment_date','max')); vclaims=vclaims.merge(paysum,on='claim_id',how='left').fillna({'total_payment':0,'payer_payment':0,'patient_payment':0,'payment_count':0}); vclaims['expected_collection']=vclaims['allowed_amount']+vclaims['patient_responsibility']; vclaims['payment_difference']=vclaims['total_payment']-vclaims['expected_collection']; vclaims['reconciliation_status']=np.where(vclaims['payment_difference'].abs()<=CFG['claim_payment_tolerance'],'Matched','Mismatch'); vclaims['denied_flag']=vclaims['claim_status'].eq('Denied'); save_gold('fact_claims',vclaims)
    vcl=clines[clines['claim_id'].isin(vclaims['claim_id']) & clines['procedure_code'].isin(proc['procedure_code']) & clines['charge_amount'].notna()].merge(proc,on='procedure_code',how='left'); save_gold('fact_claim_lines',vcl)
    revenue=vclaims.groupby(['payer_id','payer_name','payer_type'],as_index=False).agg(claim_count=('claim_id','count'),denied_claims=('denied_flag','sum'),total_charge=('total_charge','sum'),allowed_amount=('allowed_amount','sum'),total_payment=('total_payment','sum'),patient_responsibility=('patient_responsibility','sum')); revenue['denial_rate']=safe_div(revenue['denied_claims'],revenue['claim_count']); revenue['collection_rate']=safe_div(revenue['total_payment'],revenue['allowed_amount']+revenue['patient_responsibility']); save_gold('mart_payer_performance',revenue)
    rcm=pd.DataFrame([{'claim_count':len(vclaims),'total_charge':vclaims['total_charge'].sum(),'allowed_amount':vclaims['allowed_amount'].sum(),'total_payment':vclaims['total_payment'].sum(),'denied_claims':int(vclaims['denied_flag'].sum()),'denial_rate':float(vclaims['denied_flag'].mean()),'payment_mismatches':int((vclaims['reconciliation_status']=='Mismatch').sum())}]); save_gold('mart_revenue_cycle_summary',rcm)

    # beds
    vbeds=beds[beds['facility_id'].isin(fac['facility_id']) & beds['department_id'].isin(dep['department_id'])].copy(); vbeds['high_occupancy_flag']=vbeds['occupancy_pct']>=CFG['bed_occupancy_alert_pct']; save_gold('fact_bed_occupancy',vbeds)
    bedmart=vbeds.groupby(['facility_id','department_id'],as_index=False).agg(avg_occupancy_pct=('occupancy_pct','mean'),max_occupancy_pct=('occupancy_pct','max'),high_occupancy_days=('high_occupancy_flag','sum'),avg_boarding_patients=('boarding_patients','mean')); bedmart=bedmart.merge(dep[['department_id','department_name']],on='department_id',how='left').merge(fac[['facility_id','facility_name']],on='facility_id',how='left'); save_gold('mart_bed_utilization',bedmart)

    # appointments
    vappts=appts[appts['patient_id'].isin(patients['patient_id']) & appts['provider_id'].isin(prv['provider_id']) & appts['scheduled_ts'].notna()].copy(); vappts['no_show_flag']=vappts['appointment_status'].eq('No Show'); vappts['cancelled_flag']=vappts['appointment_status'].eq('Cancelled'); save_gold('fact_appointments',vappts)
    access=vappts.groupby(['facility_id','department_id','appointment_type'],as_index=False).agg(appointment_count=('appointment_id','count'),completed=('appointment_status',lambda x:(x=='Completed').sum()),no_shows=('no_show_flag','sum'),cancelled=('cancelled_flag','sum')); access['no_show_rate']=safe_div(access['no_shows'],access['appointment_count']); access['completion_rate']=safe_div(access['completed'],access['appointment_count']); save_gold('mart_appointment_access',access)

    # patient experience
    vsat=sat[sat['encounter_id'].isin(vf['encounter_id']) & sat['overall_rating'].notna()].merge(vf[['encounter_id','facility_id','department_id','provider_id']],on='encounter_id',how='left'); save_gold('fact_patient_satisfaction',vsat)
    exp=vsat.groupby(['facility_id','department_id'],as_index=False).agg(survey_count=('survey_id','count'),avg_overall_rating=('overall_rating','mean'),avg_communication=('communication_rating','mean'),avg_cleanliness=('cleanliness_rating','mean'),recommend_rate=('would_recommend',lambda x:pd.Series(x).fillna(False).astype(bool).mean())); save_gold('mart_patient_experience',exp)

    # provider performance
    encperf=vf.groupby('provider_id',as_index=False).agg(encounter_count=('encounter_id','count'),inpatient_count=('inpatient_flag','sum'),avg_los=('length_of_stay_days','mean'),high_los_count=('high_los_flag','sum')); satperf=vsat.groupby('provider_id',as_index=False).agg(avg_patient_rating=('overall_rating','mean'),survey_count=('survey_id','count')); providerperf=prv[['provider_id','provider_name','specialty','facility_id','department_id']].merge(encperf,on='provider_id',how='left').merge(satperf,on='provider_id',how='left').fillna({'encounter_count':0,'inpatient_count':0,'avg_los':0,'high_los_count':0,'avg_patient_rating':0,'survey_count':0}); save_gold('mart_provider_performance',providerperf)

    # department quality
    dept_enc=vf.groupby(['facility_id','department_id'],as_index=False).agg(encounter_count=('encounter_id','count'),avg_los=('length_of_stay_days','mean'),high_los_count=('high_los_flag','sum')); dept_read=inpatient.groupby(['facility_id','department_id'],as_index=False).agg(inpatient_encounters=('encounter_id','count'),readmissions_30d=('readmission_30d_flag','sum')); dq=dept_enc.merge(dept_read,on=['facility_id','department_id'],how='left').merge(exp,on=['facility_id','department_id'],how='left').merge(bedmart[['facility_id','department_id','avg_occupancy_pct','high_occupancy_days']],on=['facility_id','department_id'],how='left').fillna(0); dq['readmission_rate_30d']=safe_div(dq['readmissions_30d'],dq['inpatient_encounters']); save_gold('mart_department_quality',dq)

    # patient 360
    enc_c=vf.groupby('patient_id',as_index=False).agg(encounter_count=('encounter_id','count'),inpatient_count=('inpatient_flag','sum'),total_los_days=('length_of_stay_days','sum')); claim_c=vclaims.groupby('patient_id',as_index=False).agg(claim_count=('claim_id','count'),total_charge=('total_charge','sum'),allowed_amount=('allowed_amount','sum'),total_payment=('total_payment','sum'),denied_claims=('denied_flag','sum')); appt_c=vappts.groupby('patient_id',as_index=False).agg(appointment_count=('appointment_id','count'),no_show_count=('no_show_flag','sum')); p360=patients.merge(enc_c,on='patient_id',how='left').merge(claim_c,on='patient_id',how='left').merge(appt_c,on='patient_id',how='left').fillna({'encounter_count':0,'inpatient_count':0,'total_los_days':0,'claim_count':0,'total_charge':0,'allowed_amount':0,'total_payment':0,'denied_claims':0,'appointment_count':0,'no_show_count':0}); p360['no_show_rate']=safe_div(p360['no_show_count'],p360['appointment_count']); save_gold('mart_patient_360',p360)

    # exception marts
    exceptions={'exception_high_los':vf[vf['high_los_flag']], 'exception_readmissions_30d':inpatient[inpatient['readmission_30d_flag']], 'exception_denied_claims':vclaims[vclaims['denied_flag']], 'exception_claim_payment_mismatch':vclaims[vclaims['reconciliation_status']=='Mismatch'], 'exception_high_bed_occupancy':vbeds[vbeds['high_occupancy_flag']], 'exception_lab_turnaround':vlabs[vlabs['turnaround_alert_flag']]}
    for n,d in exceptions.items(): save_gold(n,d)

    dqrows=[]
    for n,c in raw_counts.items(): dqrows.append({'check_name':f'{n}_raw_to_silver_delta','table_name':n,'issue_count':c-len(data[n]),'severity':'info'})
    for n,c in qcounts.items(): dqrows.append({'check_name':n,'table_name':n.split('_')[0],'issue_count':c,'severity':'warning' if c else 'pass'})
    write_csv(pd.DataFrame(dqrows),REPORT/'data_quality_report.csv')
    audit=pd.DataFrame([{'pipeline_name':CFG['project_name'],'run_timestamp_utc':pd.Timestamp.utcnow().isoformat(),'raw_file_count':len(RAW),'raw_total_rows':sum(raw_counts.values()),'silver_total_rows':sum(len(v) for v in data.values()),'quarantine_total_rows':sum(qcounts.values()),'gold_table_count':len(list(GOLD.glob('*.csv'))),'status':'SUCCESS'}]); write_csv(audit,REPORT/'pipeline_audit_log.csv')
    meta={'last_successful_run_utc':pd.Timestamp.utcnow().isoformat(),'encounter_admit_watermark':str(enc['admit_ts'].max()),'appointment_watermark':str(appts['scheduled_ts'].max()),'claim_submission_watermark':str(claims['claim_submission_date'].max()),'status':'SUCCESS'}; (META/'run_watermarks.json').write_text(json.dumps(meta,indent=2),encoding='utf-8')
    print(f'Healthcare ETL complete. Gold={len(list(GOLD.glob("*.csv")))} Quarantine={len(list(Q.glob("*.csv")))}')

if __name__=='__main__': main()
