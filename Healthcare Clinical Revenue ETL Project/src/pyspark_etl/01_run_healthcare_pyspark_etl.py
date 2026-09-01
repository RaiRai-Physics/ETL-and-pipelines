from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT=Path(__file__).resolve().parents[2]
RAW=ROOT/'data/raw'; SILVER=ROOT/'data/silver/pyspark'; GOLD=ROOT/'data/gold/pyspark'; Q=ROOT/'data/quarantine/pyspark'; REPORT=ROOT/'reports/pyspark'

def spark(): return SparkSession.builder.appName('HealthcareClinicalRevenueETL').master('local[*]').getOrCreate()
def read(s,rel): return s.read.option('header',True).option('inferSchema',False).csv(str(RAW/rel))
def write(df,path): df.coalesce(1).write.mode('overwrite').option('header',True).csv(str(path))
def txt(c,default='Unknown',case=None):
    v=F.trim(F.coalesce(F.col(c),F.lit(''))); v=F.when(v=='',F.lit(default)).otherwise(v)
    return F.upper(v) if case=='upper' else F.lower(v) if case=='lower' else F.initcap(v) if case=='title' else v
def num(c,default=None):
    n=F.regexp_replace(F.regexp_replace(F.trim(F.coalesce(F.col(c),F.lit(''))),'\\$',''),',','').cast('double')
    return F.coalesce(n,F.lit(float(default))) if default is not None else n
def ts(c):
    v=F.trim(F.coalesce(F.col(c),F.lit('')))
    return F.coalesce(F.to_timestamp(v,'yyyy-MM-dd HH:mm:ss'),F.to_timestamp(v,'MM/dd/yyyy HH:mm'))
def dt(c):
    v=F.trim(F.coalesce(F.col(c),F.lit('')))
    return F.coalesce(F.to_date(v,'yyyy-MM-dd'),F.to_date(v,'MM/dd/yyyy'))

def main():
    s=spark()
    patients=(read(s,'master/patients.csv').withColumn('patient_id',txt('patient_id','', 'upper')).withColumn('primary_payer_id',txt('primary_payer_id','', 'upper')).withColumn('date_of_birth',dt('date_of_birth')).dropDuplicates(['patient_id']))
    providers=(read(s,'master/providers.csv').withColumn('provider_id',txt('provider_id','', 'upper')).withColumn('facility_id',txt('facility_id','', 'upper')).withColumn('department_id',txt('department_id','', 'upper')).withColumn('specialty',txt('specialty',case='title')).dropDuplicates(['provider_id']))
    encounters=(read(s,'clinical/encounters.csv').withColumn('encounter_id',txt('encounter_id','', 'upper')).withColumn('patient_id',txt('patient_id','', 'upper')).withColumn('provider_id',txt('provider_id','', 'upper')).withColumn('facility_id',txt('facility_id','', 'upper')).withColumn('department_id',txt('department_id','', 'upper')).withColumn('encounter_type',txt('encounter_type',case='title')).withColumn('admit_ts',ts('admit_ts')).withColumn('discharge_ts',ts('discharge_ts')).withColumn('length_of_stay_days',(F.col('discharge_ts').cast('long')-F.col('admit_ts').cast('long'))/F.lit(86400.0)).dropDuplicates(['encounter_id']))
    claims=(read(s,'revenue/claims.csv').withColumn('claim_id',txt('claim_id','', 'upper')).withColumn('encounter_id',txt('encounter_id','', 'upper')).withColumn('payer_id',txt('payer_id','', 'upper')).withColumn('claim_status',txt('claim_status',case='title')).withColumn('total_charge',num('total_charge')).withColumn('allowed_amount',num('allowed_amount',0)).withColumn('patient_responsibility',num('patient_responsibility',0)).dropDuplicates(['claim_id']))
    labs=(read(s,'clinical/lab_results.csv').withColumn('lab_result_id',txt('lab_result_id','', 'upper')).withColumn('lab_order_id',txt('lab_order_id','', 'upper')).withColumn('result_ts',ts('result_ts')).withColumn('result_value',num('result_value')).dropDuplicates(['lab_result_id']))
    lorders=(read(s,'clinical/lab_orders.csv').withColumn('lab_order_id',txt('lab_order_id','', 'upper')).withColumn('encounter_id',txt('encounter_id','', 'upper')).withColumn('lab_test_id',txt('lab_test_id','', 'upper')).withColumn('order_ts',ts('order_ts')).dropDuplicates(['lab_order_id']))

    for n,df in {'patients':patients,'providers':providers,'encounters':encounters,'claims':claims,'lab_results':labs,'lab_orders':lorders}.items(): write(df,SILVER/n)

    bad_enc_patient=encounters.join(patients.select('patient_id'),'patient_id','left_anti')
    bad_enc_provider=encounters.join(providers.select('provider_id'),'provider_id','left_anti')
    bad_claim_enc=claims.join(encounters.select('encounter_id'),'encounter_id','left_anti')
    bad_lab_order=labs.join(lorders.select('lab_order_id'),'lab_order_id','left_anti')
    for n,df in {'encounters_invalid_patient':bad_enc_patient,'encounters_invalid_provider':bad_enc_provider,'claims_invalid_encounter':bad_claim_enc,'lab_results_invalid_order':bad_lab_order}.items(): write(df,Q/n)

    trusted_enc=(encounters.join(patients.select('patient_id','date_of_birth'),'patient_id','inner').join(providers.select('provider_id'),'provider_id','inner').filter(F.col('admit_ts').isNotNull()).withColumn('age_at_encounter',F.floor(F.datediff(F.to_date('admit_ts'),'date_of_birth')/F.lit(365.25))).withColumn('high_los_flag',F.col('length_of_stay_days')>F.lit(7)))
    inpatient=(trusted_enc.filter((F.col('encounter_type')=='Inpatient')&F.col('discharge_ts').isNotNull()).withColumn('previous_discharge_ts',F.lag('discharge_ts').over(__import__('pyspark').sql.Window.partitionBy('patient_id').orderBy('admit_ts'))).withColumn('days_since_previous_discharge',(F.col('admit_ts').cast('long')-F.col('previous_discharge_ts').cast('long'))/F.lit(86400.0)).withColumn('readmission_30d_flag',F.col('days_since_previous_discharge').between(0,30)))
    lab_fact=(labs.join(lorders,'lab_order_id','inner').withColumn('turnaround_hours',(F.col('result_ts').cast('long')-F.col('order_ts').cast('long'))/F.lit(3600.0)).withColumn('turnaround_alert_flag',F.col('turnaround_hours')>F.lit(12)))
    daily=(trusted_enc.groupBy(F.to_date('admit_ts').alias('admit_date'),'facility_id','encounter_type').agg(F.count('encounter_id').alias('encounter_count'),F.avg('length_of_stay_days').alias('avg_los'),F.sum(F.col('high_los_flag').cast('int')).alias('high_los_count')))
    for n,df in {'fact_encounters':trusted_enc,'mart_readmissions_30d':inpatient,'fact_lab_results':lab_fact,'mart_daily_encounter_volume':daily}.items(): write(df,GOLD/n)
    dq=s.createDataFrame([('encounters_invalid_patient',bad_enc_patient.count()),('encounters_invalid_provider',bad_enc_provider.count()),('claims_invalid_encounter',bad_claim_enc.count()),('lab_results_invalid_order',bad_lab_order.count())],['check_name','issue_count']); write(dq,REPORT/'data_quality_report')
    print('PySpark healthcare ETL complete.'); s.stop()
if __name__=='__main__': main()
