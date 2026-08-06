from pathlib import Path
from pyspark.sql import SparkSession,functions as F
ROOT=Path(__file__).resolve().parents[2]; RAW=ROOT/'data/raw'; SILVER=ROOT/'data/silver/pyspark'; GOLD=ROOT/'data/gold/pyspark'; QUAR=ROOT/'data/quarantine/pyspark'; REPORT=ROOT/'reports/pyspark'
def spark(): return SparkSession.builder.appName('SmartGridEnergyETL').master('local[*]').getOrCreate()
def read(s,p): return s.read.option('header',True).option('inferSchema',False).csv(str(RAW/p))
def write(df,p): df.coalesce(1).write.mode('overwrite').option('header',True).csv(str(p))
def text(c,d='Unknown',case=None):
    v=F.trim(F.coalesce(F.col(c),F.lit(''))); v=F.when(v=='',d).otherwise(v)
    return F.upper(v) if case=='upper' else F.lower(v) if case=='lower' else F.initcap(v) if case=='title' else v
def num(c): return F.regexp_replace(F.regexp_replace(F.trim(F.coalesce(F.col(c),F.lit(''))),'\\$',''),',','').cast('double')
def ts(c): return F.coalesce(F.to_timestamp(c,'yyyy-MM-dd HH:mm:ss'),F.to_timestamp(c,'MM/dd/yyyy HH:mm'))
def main():
    s=spark()
    meters=read(s,'master/meters.csv').withColumn('meter_id',text('meter_id','', 'upper')).withColumn('customer_id',text('customer_id','', 'upper')).withColumn('site_id',text('site_id','', 'upper')).withColumn('multiplier',num('multiplier')).dropDuplicates(['meter_id'])
    customers=read(s,'master/customers.csv').withColumn('customer_id',text('customer_id','', 'upper')).withColumn('segment',text('segment','Unknown','title')).withColumn('region',text('region','Unknown','title')).dropDuplicates(['customer_id'])
    mr=read(s,'telemetry/meter_readings.csv').withColumn('reading_id',text('reading_id','', 'upper')).withColumn('meter_id',text('meter_id','', 'upper')).withColumn('reading_ts',ts('reading_ts')).withColumn('reading_date',F.to_date('reading_ts')).withColumn('kwh',num('kwh')).withColumn('kw_demand',num('kw_demand')).withColumn('quality_flag',text('quality_flag','Unknown','upper')).dropDuplicates(['reading_id'])
    assets=read(s,'master/assets.csv').withColumn('asset_id',text('asset_id','', 'upper')).withColumn('site_id',text('site_id','', 'upper')).withColumn('asset_type',text('asset_type','Unknown','title')).withColumn('criticality',text('criticality','Unknown','title')).dropDuplicates(['asset_id'])
    at=read(s,'telemetry/asset_telemetry.csv').withColumn('telemetry_id',text('telemetry_id','', 'upper')).withColumn('asset_id',text('asset_id','', 'upper')).withColumn('reading_ts',ts('reading_ts')).withColumn('temperature_c',num('temperature_c')).withColumn('vibration_mm_s',num('vibration_mm_s')).withColumn('power_output_kw',num('power_output_kw')).withColumn('status_code',text('status_code','Unknown','upper')).dropDuplicates(['telemetry_id'])
    for n,df in {'meters':meters,'customers':customers,'meter_readings':mr,'assets':assets,'asset_telemetry':at}.items(): write(df,SILVER/n)
    bad_mr=mr.join(meters.select('meter_id'),'meter_id','left_anti').unionByName(mr.filter(F.col('reading_ts').isNull()|F.col('kwh').isNull()|(F.col('kwh')<0)),allowMissingColumns=True); write(bad_mr,QUAR/'meter_readings_bad')
    valid=mr.join(meters,'meter_id','inner').join(customers,'customer_id','left').filter(F.col('reading_ts').isNotNull()&F.col('kwh').isNotNull()&(F.col('kwh')>=0)).withColumn('adjusted_kwh',F.col('kwh')*F.coalesce(F.col('multiplier'),F.lit(1.0)))
    daily=valid.groupBy('reading_date','region','segment').agg(F.countDistinct('meter_id').alias('meter_count'),F.countDistinct('customer_id').alias('customer_count'),F.sum('adjusted_kwh').alias('consumption_kwh'),F.max('kw_demand').alias('peak_demand_kw'))
    health=at.join(assets,'asset_id','inner').filter(F.col('reading_ts').isNotNull()&F.col('power_output_kw').isNotNull()).withColumn('anomaly_flag',(F.col('temperature_c')>=75)|(F.col('vibration_mm_s')>=5)|F.col('status_code').isin('WARNING','TRIP')).groupBy('asset_id','site_id','asset_type','criticality').agg(F.count('*').alias('reading_count'),F.avg('temperature_c').alias('avg_temperature_c'),F.max('temperature_c').alias('max_temperature_c'),F.sum(F.col('anomaly_flag').cast('int')).alias('anomaly_count'))
    write(valid,GOLD/'fact_meter_readings'); write(daily,GOLD/'mart_daily_consumption'); write(health,GOLD/'mart_asset_health')
    dq=s.createDataFrame([('meter_readings_bad',bad_mr.count())],['check_name','issue_count']); write(dq,REPORT/'data_quality_report'); print('PySpark smart grid ETL complete.'); s.stop()
if __name__=='__main__': main()
