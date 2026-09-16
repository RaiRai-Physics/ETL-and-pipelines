from pathlib import Path
from pyspark.sql import SparkSession,functions as F
ROOT=Path(__file__).resolve().parents[2]; RAW=ROOT/'data/raw'; SI=ROOT/'data/silver/pyspark'; GO=ROOT/'data/gold/pyspark'; Q=ROOT/'data/quarantine/pyspark'
def sp(): return SparkSession.builder.appName('Telecom5GMedallionETL').master('local[*]').getOrCreate()
def read(s,p): return s.read.option('header',True).option('inferSchema',False).csv(str(RAW/p))
def write(df,p): df.coalesce(1).write.mode('overwrite').option('header',True).csv(str(p))
def txt(c,d='Unknown',case=None):
    v=F.trim(F.coalesce(F.col(c),F.lit(''))); v=F.when(v=='',d).otherwise(v); return F.upper(v) if case=='upper' else F.initcap(v) if case=='title' else v
def num(c): return F.col(c).cast('double')
def tstamp(c):
    v=F.trim(F.coalesce(F.col(c),F.lit(''))); return F.coalesce(F.to_timestamp(v,'yyyy-MM-dd HH:mm:ss'),F.to_timestamp(v,'MM/dd/yyyy HH:mm'))
def main():
    s=sp(); subs=read(s,'master/subscribers.csv').withColumn('subscriber_id',txt('subscriber_id','', 'upper')).withColumn('customer_id',txt('customer_id','', 'upper')).withColumn('plan_id',txt('plan_id','', 'upper')).dropDuplicates(['subscriber_id'])
    cells=read(s,'master/cells.csv').withColumn('cell_id',txt('cell_id','', 'upper')).withColumn('site_id',txt('site_id','', 'upper')).withColumn('max_capacity_mbps',num('max_capacity_mbps')).dropDuplicates(['cell_id'])
    voice=read(s,'usage/voice_calls.csv').withColumn('call_id',txt('call_id','', 'upper')).withColumn('subscriber_id',txt('subscriber_id','', 'upper')).withColumn('cell_id',txt('cell_id','', 'upper')).withColumn('call_start_ts',tstamp('call_start_ts')).withColumn('duration_seconds',num('duration_seconds')).withColumn('call_status',txt('call_status',case='title')).dropDuplicates(['call_id'])
    data=read(s,'usage/data_sessions.csv').withColumn('session_id',txt('session_id','', 'upper')).withColumn('subscriber_id',txt('subscriber_id','', 'upper')).withColumn('cell_id',txt('cell_id','', 'upper')).withColumn('session_start_ts',tstamp('session_start_ts')).withColumn('data_mb',num('data_mb')).withColumn('latency_ms',num('latency_ms')).withColumn('packet_loss_pct',num('packet_loss_pct')).dropDuplicates(['session_id'])
    kpi=read(s,'network/cell_kpis.csv').withColumn('cell_id',txt('cell_id','', 'upper')).withColumn('kpi_ts',tstamp('kpi_ts')).withColumn('prb_utilization_pct',num('prb_utilization_pct')).withColumn('drop_call_rate_pct',num('drop_call_rate_pct')).withColumn('availability_pct',num('availability_pct')).dropDuplicates(['cell_id','kpi_ts'])
    for n,df in {'subscribers':subs,'cells':cells,'voice_calls':voice,'data_sessions':data,'cell_kpis':kpi}.items(): write(df,SI/n)
    bv=voice.join(subs.select('subscriber_id'),'subscriber_id','left_anti'); bd=data.join(subs.select('subscriber_id'),'subscriber_id','left_anti'); bc=kpi.join(cells.select('cell_id'),'cell_id','left_anti'); write(bv,Q/'voice_invalid_subscriber'); write(bd,Q/'data_invalid_subscriber'); write(bc,Q/'kpis_invalid_cell')
    vf=voice.join(subs,'subscriber_id','inner').join(cells,'cell_id','inner').filter(F.col('duration_seconds')>0).withColumn('duration_minutes',F.col('duration_seconds')/60).withColumn('dropped_call_flag',F.col('call_status')=='Dropped')
    df=data.join(subs,'subscriber_id','inner').join(cells,'cell_id','inner').filter(F.col('data_mb')>=0).withColumn('data_gb',F.col('data_mb')/1024).withColumn('poor_qoe_flag',(F.col('latency_ms')>=80)|(F.col('packet_loss_pct')>=2))
    kh=kpi.join(cells,'cell_id','inner').withColumn('high_utilization_flag',F.col('prb_utilization_pct')>=85).withColumn('high_drop_rate_flag',F.col('drop_call_rate_pct')>=3).withColumn('poor_availability_flag',F.col('availability_pct')<99)
    ch=kh.groupBy('cell_id','site_id','technology').agg(F.avg('prb_utilization_pct').alias('avg_prb_utilization_pct'),F.avg('drop_call_rate_pct').alias('avg_drop_call_rate_pct'),F.avg('availability_pct').alias('avg_availability_pct'),F.sum(F.col('high_utilization_flag').cast('int')).alias('high_util_hours'))
    write(vf,GO/'fact_voice_calls'); write(df,GO/'fact_data_sessions'); write(kh,GO/'fact_cell_kpis'); write(ch,GO/'mart_cell_network_health')
    dq=s.createDataFrame([('voice_invalid_subscriber',bv.count()),('data_invalid_subscriber',bd.count()),('kpis_invalid_cell',bc.count())],['check_name','issue_count']); write(dq,ROOT/'reports/pyspark/data_quality_report'); print('PySpark core medallion ETL complete'); s.stop()
if __name__=='__main__': main()
