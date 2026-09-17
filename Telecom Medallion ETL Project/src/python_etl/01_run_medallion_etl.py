from pathlib import Path
import sys,json
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parents[2]; sys.path.append(str(ROOT))
from src.common.utils import config,read_csv,write_csv,clean_text,num,dt,ts,b,h,safe_div
CFG=config(); BR=ROOT/CFG['bronze_path']; SI=ROOT/CFG['silver_path']; Q=ROOT/CFG['quarantine_path']; GO=ROOT/CFG['gold_path']; REP=ROOT/CFG['report_path']; META=ROOT/CFG['metadata_path']
RAW={
'customers':'data/raw/master/customers.csv','plans':'data/raw/master/plans.csv','devices':'data/raw/master/devices.csv','subscribers':'data/raw/master/subscribers.csv','sites':'data/raw/master/sites.csv','cells':'data/raw/master/cells.csv','calendar':'data/raw/reference/calendar.csv',
'voice_calls':'data/raw/usage/voice_calls.csv','data_sessions':'data/raw/usage/data_sessions.csv','sms_events':'data/raw/usage/sms_events.csv','cell_kpis':'data/raw/network/cell_kpis.csv','network_alarms':'data/raw/network/network_alarms.csv','site_outages':'data/raw/network/site_outages.csv',
'invoices':'data/raw/billing/invoices.csv','payments':'data/raw/billing/payments.csv','support_tickets':'data/raw/support/support_tickets.csv','customer_cdc':'data/raw/cdc/customer_cdc.csv','plan_cdc':'data/raw/cdc/plan_cdc.csv'}

def raw(n): return read_csv(ROOT/RAW[n])
def bronze(n,df):
    x=df.copy(); x['_ingested_at_utc']=pd.Timestamp.utcnow().isoformat(); x['_source_file']=RAW[n]; x['_source_system']=n; write_csv(x,BR/f'{n}.csv')
def silver(n,df): write_csv(df,SI/f'{n}.csv')
def gold(n,df): write_csv(df,GO/f'{n}.csv')
def quarantine(n,df,reason):
    x=df.copy(); x['quarantine_reason']=reason
    if len(x)==0: x=pd.DataFrame(columns=['quarantine_reason'])
    write_csv(x,Q/f'{n}.csv')

def c_customers(x):
    x=x.copy(); x['customer_id']=clean_text(x['customer_id'],'','upper'); x['customer_name']=clean_text(x['customer_name'],case='title'); x['customer_segment']=clean_text(x['customer_segment'],'Unknown','title'); x['city']=clean_text(x['city'],case='title'); x['region']=clean_text(x['region'],case='title'); x['signup_date']=dt(x['signup_date']); x['credit_class']=clean_text(x['credit_class'],'Unknown','upper'); x['autopay_flag']=b(x['autopay_flag']); x['active_flag']=b(x['active_flag']); return x.drop_duplicates('customer_id')
def c_plans(x):
    x=x.copy(); x['plan_id']=clean_text(x['plan_id'],'','upper'); x['plan_name']=clean_text(x['plan_name'],case='title'); x['plan_type']=clean_text(x['plan_type'],case='title'); x['monthly_fee']=num(x['monthly_fee']); x['included_voice_minutes']=num(x['included_voice_minutes'],0); x['included_sms']=num(x['included_sms'],0); x['included_data_gb']=num(x['included_data_gb'],0); x['overage_per_gb']=num(x['overage_per_gb'],0); x['active_flag']=b(x['active_flag']); return x.drop_duplicates('plan_id')
def c_devices(x):
    x=x.copy(); x['device_id']=clean_text(x['device_id'],'','upper'); x['customer_id']=clean_text(x['customer_id'],'','upper'); x['device_type']=clean_text(x['device_type'],case='title'); x['manufacturer']=clean_text(x['manufacturer'],case='title'); x['activation_date']=dt(x['activation_date']); x['5g_capable_flag']=b(x['5g_capable_flag']); x['device_status']=clean_text(x['device_status'],case='title'); return x.drop_duplicates('device_id')
def c_subscribers(x):
    x=x.copy(); x['subscriber_id']=clean_text(x['subscriber_id'],'','upper'); x['customer_id']=clean_text(x['customer_id'],'','upper'); x['device_id']=clean_text(x['device_id'],'','upper'); x['plan_id']=clean_text(x['plan_id'],'','upper'); x['start_date']=dt(x['start_date']); x['subscriber_status']=clean_text(x['subscriber_status'],case='title'); x['home_region']=clean_text(x['home_region'],case='title'); return x.drop_duplicates('subscriber_id')
def c_sites(x):
    x=x.copy(); x['site_id']=clean_text(x['site_id'],'','upper'); x['site_name']=clean_text(x['site_name'],case='title'); x['city']=clean_text(x['city'],case='title'); x['region']=clean_text(x['region'],case='title'); x['site_type']=clean_text(x['site_type'],case='title'); x['active_flag']=b(x['active_flag']); return x.drop_duplicates('site_id')
def c_cells(x):
    x=x.copy(); x['cell_id']=clean_text(x['cell_id'],'','upper'); x['site_id']=clean_text(x['site_id'],'','upper'); x['technology']=clean_text(x['technology'],case='upper'); x['max_capacity_mbps']=num(x['max_capacity_mbps']); x['active_flag']=b(x['active_flag']); return x.drop_duplicates('cell_id')
def c_calendar(x): x=x.copy(); x['calendar_date']=dt(x['calendar_date']); return x.drop_duplicates('date_key')
def c_voice(x):
    x=x.copy(); x['call_id']=clean_text(x['call_id'],'','upper'); x['subscriber_id']=clean_text(x['subscriber_id'],'','upper'); x['cell_id']=clean_text(x['cell_id'],'','upper'); x['call_start_ts']=ts(x['call_start_ts']); x['call_date']=x['call_start_ts'].dt.date.astype('string'); x['duration_seconds']=num(x['duration_seconds']); x['call_direction']=clean_text(x['call_direction'],case='upper'); x['call_status']=clean_text(x['call_status'],case='title'); x['roaming_flag']=b(x['roaming_flag']); return x.drop_duplicates('call_id')
def c_data(x):
    x=x.copy(); x['session_id']=clean_text(x['session_id'],'','upper'); x['subscriber_id']=clean_text(x['subscriber_id'],'','upper'); x['cell_id']=clean_text(x['cell_id'],'','upper'); x['session_start_ts']=ts(x['session_start_ts']); x['session_end_ts']=ts(x['session_end_ts']); x['session_date']=x['session_start_ts'].dt.date.astype('string'); x['data_mb']=num(x['data_mb']); x['avg_downlink_mbps']=num(x['avg_downlink_mbps']); x['avg_uplink_mbps']=num(x['avg_uplink_mbps']); x['latency_ms']=num(x['latency_ms']); x['packet_loss_pct']=num(x['packet_loss_pct']); x['roaming_flag']=b(x['roaming_flag']); return x.drop_duplicates('session_id')
def c_sms(x):
    x=x.copy(); x['sms_id']=clean_text(x['sms_id'],'','upper'); x['subscriber_id']=clean_text(x['subscriber_id'],'','upper'); x['cell_id']=clean_text(x['cell_id'],'','upper'); x['sms_ts']=ts(x['sms_ts']); x['sms_date']=x['sms_ts'].dt.date.astype('string'); x['direction']=clean_text(x['direction'],case='upper'); x['delivery_status']=clean_text(x['delivery_status'],case='title'); x['roaming_flag']=b(x['roaming_flag']); return x.drop_duplicates('sms_id')
def c_kpis(x):
    x=x.copy(); x['cell_id']=clean_text(x['cell_id'],'','upper'); x['kpi_ts']=ts(x['kpi_ts']); x['kpi_date']=x['kpi_ts'].dt.date.astype('string');
    for c in ['connected_users','throughput_mbps','prb_utilization_pct','drop_call_rate_pct','handover_success_pct','availability_pct']: x[c]=num(x[c])
    return x.drop_duplicates(['cell_id','kpi_ts'])
def c_alarms(x):
    x=x.copy(); x['alarm_id']=clean_text(x['alarm_id'],'','upper'); x['cell_id']=clean_text(x['cell_id'],'','upper'); x['alarm_ts']=ts(x['alarm_ts']); x['cleared_ts']=ts(x['cleared_ts']); x['severity']=clean_text(x['severity'],case='title'); x['alarm_type']=clean_text(x['alarm_type'],case='title'); x['alarm_status']=clean_text(x['alarm_status'],case='title'); x['alarm_duration_hours']=(x['cleared_ts']-x['alarm_ts']).dt.total_seconds()/3600; return x.drop_duplicates('alarm_id')
def c_outages(x):
    x=x.copy(); x['outage_id']=clean_text(x['outage_id'],'','upper'); x['site_id']=clean_text(x['site_id'],'','upper'); x['outage_start_ts']=ts(x['outage_start_ts']); x['outage_end_ts']=ts(x['outage_end_ts']); x['outage_type']=clean_text(x['outage_type'],case='title'); x['customers_affected']=num(x['customers_affected'],0); x['status']=clean_text(x['status'],case='title'); x['duration_hours']=(x['outage_end_ts']-x['outage_start_ts']).dt.total_seconds()/3600; return x.drop_duplicates('outage_id')
def c_invoices(x):
    x=x.copy(); x['invoice_id']=clean_text(x['invoice_id'],'','upper'); x['subscriber_id']=clean_text(x['subscriber_id'],'','upper'); x['billing_month']=dt(x['billing_month']); x['invoice_date']=dt(x['invoice_date']); x['due_date']=dt(x['due_date']);
    for c in ['monthly_charge','usage_charge','tax_amount','other_fees','total_amount']: x[c]=num(x[c])
    x['invoice_status']=clean_text(x['invoice_status'],case='title'); return x.drop_duplicates('invoice_id')
def c_payments(x):
    x=x.copy(); x['payment_id']=clean_text(x['payment_id'],'','upper'); x['invoice_id']=clean_text(x['invoice_id'],'','upper'); x['payment_date']=dt(x['payment_date']); x['payment_amount']=num(x['payment_amount']); x['payment_method']=clean_text(x['payment_method'],case='upper'); x['payment_status']=clean_text(x['payment_status'],case='title'); return x.drop_duplicates('payment_id')
def c_tickets(x):
    x=x.copy(); x['ticket_id']=clean_text(x['ticket_id'],'','upper'); x['customer_id']=clean_text(x['customer_id'],'','upper'); x['opened_ts']=ts(x['opened_ts']); x['resolved_ts']=ts(x['resolved_ts']); x['ticket_category']=clean_text(x['ticket_category'],case='title'); x['priority']=clean_text(x['priority'],case='title'); x['ticket_status']=clean_text(x['ticket_status'],case='title'); x['channel']=clean_text(x['channel'],case='title'); x['resolution_hours']=(x['resolved_ts']-x['opened_ts']).dt.total_seconds()/3600; return x.drop_duplicates('ticket_id')
def c_customer_cdc(x):
    x=x.copy(); x['cdc_id']=clean_text(x['cdc_id'],'','upper'); x['customer_id']=clean_text(x['customer_id'],'','upper'); x['effective_ts']=ts(x['effective_ts']); x['effective_date']=x['effective_ts'].dt.date.astype('string'); x['new_segment']=clean_text(x['new_segment'],case='title'); x['new_credit_class']=clean_text(x['new_credit_class'],case='upper'); x['new_autopay_flag']=b(x['new_autopay_flag']); return x.drop_duplicates('cdc_id')
def c_plan_cdc(x):
    x=x.copy(); x['cdc_id']=clean_text(x['cdc_id'],'','upper'); x['plan_id']=clean_text(x['plan_id'],'','upper'); x['effective_ts']=ts(x['effective_ts']); x['effective_date']=x['effective_ts'].dt.date.astype('string'); x['new_monthly_fee']=num(x['new_monthly_fee']); x['new_data_gb']=num(x['new_data_gb']); return x.drop_duplicates('cdc_id')
C={'customers':c_customers,'plans':c_plans,'devices':c_devices,'subscribers':c_subscribers,'sites':c_sites,'cells':c_cells,'calendar':c_calendar,'voice_calls':c_voice,'data_sessions':c_data,'sms_events':c_sms,'cell_kpis':c_kpis,'network_alarms':c_alarms,'site_outages':c_outages,'invoices':c_invoices,'payments':c_payments,'support_tickets':c_tickets,'customer_cdc':c_customer_cdc,'plan_cdc':c_plan_cdc}

def customer_scd2(base,cdc):
    cols=['customer_id','customer_name','customer_segment','city','region','signup_date','credit_class','autopay_flag','active_flag']
    b0=base[cols].copy(); b0['effective_start_date']=b0['signup_date'].fillna('1900-01-01'); b0['source']='customer_master'
    u=cdc.merge(base[['customer_id','customer_name','city','region','signup_date','active_flag']],on='customer_id',how='inner').rename(columns={'new_segment':'customer_segment','new_credit_class':'credit_class','new_autopay_flag':'autopay_flag','effective_date':'effective_start_date'})
    u['source']='customer_cdc'; u=u[b0.columns]; x=pd.concat([b0,u],ignore_index=True); x['_d']=pd.to_datetime(x['effective_start_date'],errors='coerce'); x=x.sort_values(['customer_id','_d']); x['effective_end_date']=x.groupby('customer_id')['_d'].shift(-1).dt.date.astype('string').fillna('9999-12-31'); x['is_current']=x['effective_end_date'].eq('9999-12-31'); x['customer_sk']=[h(a,b,c,d) for a,b,c,d in zip(x['customer_id'],x['effective_start_date'],x['customer_segment'],x['credit_class'])]; return x.drop(columns='_d')
def plan_scd2(base,cdc):
    b0=base.copy(); b0['effective_start_date']='2020-01-01'; b0['source']='plan_master'
    u=cdc.merge(base.drop(columns=['monthly_fee','included_data_gb']),on='plan_id',how='inner'); u['monthly_fee']=u['new_monthly_fee']; u['included_data_gb']=u['new_data_gb']; u['effective_start_date']=u['effective_date']; u['source']='plan_cdc'; u=u[b0.columns]
    x=pd.concat([b0,u],ignore_index=True); x['_d']=pd.to_datetime(x['effective_start_date'],errors='coerce'); x=x.sort_values(['plan_id','_d']); x['effective_end_date']=x.groupby('plan_id')['_d'].shift(-1).dt.date.astype('string').fillna('9999-12-31'); x['is_current']=x['effective_end_date'].eq('9999-12-31'); x['plan_sk']=[h(a,b,c) for a,b,c in zip(x['plan_id'],x['effective_start_date'],x['monthly_fee'])]; return x.drop(columns='_d')

def main():
    for d in [BR,SI,Q,GO,REP,META]: d.mkdir(parents=True,exist_ok=True)
    D={}; raw_counts={}
    for n in RAW:
        r=raw(n); raw_counts[n]=len(r); bronze(n,r); D[n]=C[n](r); silver(n,D[n])
    cu,pl,de,su,si,ce=D['customers'],D['plans'],D['devices'],D['subscribers'],D['sites'],D['cells']; vo,da,sm=D['voice_calls'],D['data_sessions'],D['sms_events']; kp,al,ou=D['cell_kpis'],D['network_alarms'],D['site_outages']; inv,pa,ti=D['invoices'],D['payments'],D['support_tickets']
    qs={}
    def q(n,df,r): quarantine(n,df,r); qs[n]=len(df)
    q('devices_invalid_customer',de[~de.customer_id.isin(cu.customer_id)],'Missing customer')
    q('subscribers_invalid_customer',su[~su.customer_id.isin(cu.customer_id)],'Missing customer')
    q('subscribers_invalid_device',su[~su.device_id.isin(de.device_id)],'Missing device')
    q('subscribers_invalid_plan',su[~su.plan_id.isin(pl.plan_id)],'Missing plan')
    q('cells_invalid_site',ce[~ce.site_id.isin(si.site_id)],'Missing site')
    q('voice_invalid_subscriber',vo[~vo.subscriber_id.isin(su.subscriber_id)],'Missing subscriber')
    q('voice_invalid_cell',vo[~vo.cell_id.isin(ce.cell_id)],'Missing cell')
    q('voice_bad_values',vo[vo.call_start_ts.isna()|vo.duration_seconds.isna()|(vo.duration_seconds<=0)],'Bad timestamp/duration')
    q('data_invalid_subscriber',da[~da.subscriber_id.isin(su.subscriber_id)],'Missing subscriber')
    q('data_invalid_cell',da[~da.cell_id.isin(ce.cell_id)],'Missing cell')
    q('data_bad_values',da[da.session_start_ts.isna()|da.data_mb.isna()|(da.data_mb<0)],'Bad timestamp/data volume')
    q('sms_invalid_subscriber',sm[~sm.subscriber_id.isin(su.subscriber_id)],'Missing subscriber')
    q('kpis_invalid_cell',kp[~kp.cell_id.isin(ce.cell_id)],'Missing cell')
    q('alarms_invalid_cell',al[~al.cell_id.isin(ce.cell_id)],'Missing cell')
    q('outages_invalid_site',ou[~ou.site_id.isin(si.site_id)],'Missing site')
    q('invoices_invalid_subscriber',inv[~inv.subscriber_id.isin(su.subscriber_id)],'Missing subscriber')
    q('invoices_bad_values',inv[inv.total_amount.isna()],'Bad invoice amount')
    q('payments_invalid_invoice',pa[~pa.invoice_id.isin(inv.invoice_id)],'Missing invoice')
    q('payments_bad_values',pa[pa.payment_amount.isna()],'Bad payment amount')
    q('tickets_invalid_customer',ti[~ti.customer_id.isin(cu.customer_id)],'Missing customer')

    cdim=customer_scd2(cu,D['customer_cdc']); pdim=plan_scd2(pl,D['plan_cdc']); gold('dim_customer_scd2',cdim); gold('dim_customer_current',cdim[cdim.is_current]); gold('dim_plan_scd2',pdim); gold('dim_plan_current',pdim[pdim.is_current]);
    for n,df in {'dim_subscriber':su,'dim_device':de,'dim_site':si,'dim_cell':ce}.items(): gold(n,df)

    v=vo[vo.subscriber_id.isin(su.subscriber_id)&vo.cell_id.isin(ce.cell_id)&vo.call_start_ts.notna()&vo.duration_seconds.notna()&(vo.duration_seconds>0)].merge(su[['subscriber_id','customer_id','plan_id','device_id','home_region']],on='subscriber_id',how='left').merge(ce[['cell_id','site_id','technology']],on='cell_id',how='left'); v['duration_minutes']=v.duration_seconds/60; v['dropped_call_flag']=v.call_status.eq('Dropped'); gold('fact_voice_calls',v)
    d=da[da.subscriber_id.isin(su.subscriber_id)&da.cell_id.isin(ce.cell_id)&da.session_start_ts.notna()&da.data_mb.notna()&(da.data_mb>=0)].merge(su[['subscriber_id','customer_id','plan_id','device_id','home_region']],on='subscriber_id',how='left').merge(ce[['cell_id','site_id','technology']],on='cell_id',how='left'); d['data_gb']=d.data_mb/1024; d['poor_qoe_flag']=(d.latency_ms>=CFG['poor_latency_ms'])|(d.packet_loss_pct>=CFG['poor_packet_loss_pct']); gold('fact_data_sessions',d)
    s=sm[sm.subscriber_id.isin(su.subscriber_id)&sm.cell_id.isin(ce.cell_id)].merge(su[['subscriber_id','customer_id','plan_id']],on='subscriber_id',how='left'); s['failed_sms_flag']=~s.delivery_status.eq('Delivered'); gold('fact_sms_events',s)
    k=kp[kp.cell_id.isin(ce.cell_id)].merge(ce[['cell_id','site_id','technology','max_capacity_mbps']],on='cell_id',how='left'); k['high_utilization_flag']=k.prb_utilization_pct>=CFG['high_cell_utilization_pct']; k['high_drop_rate_flag']=k.drop_call_rate_pct>=CFG['high_drop_call_rate_pct']; k['poor_availability_flag']=k.availability_pct<99; gold('fact_cell_kpis',k)
    gold('fact_network_alarms',al[al.cell_id.isin(ce.cell_id)]); gold('fact_site_outages',ou[ou.site_id.isin(si.site_id)]); gold('fact_invoices',inv[inv.subscriber_id.isin(su.subscriber_id)&inv.total_amount.notna()]); gold('fact_payments',pa[pa.invoice_id.isin(inv.invoice_id)&pa.payment_amount.notna()]); gold('fact_support_tickets',ti[ti.customer_id.isin(cu.customer_id)])

    # Monthly subscriber usage
    v['month']=pd.to_datetime(v.call_date).dt.to_period('M').astype(str); d['month']=pd.to_datetime(d.session_date).dt.to_period('M').astype(str); s['month']=pd.to_datetime(s.sms_date).dt.to_period('M').astype(str)
    vu=v.groupby(['subscriber_id','month'],as_index=False).agg(voice_minutes=('duration_minutes','sum'),call_count=('call_id','count'),dropped_calls=('dropped_call_flag','sum'),roaming_calls=('roaming_flag','sum'))
    du=d.groupby(['subscriber_id','month'],as_index=False).agg(data_gb=('data_gb','sum'),data_sessions=('session_id','count'),poor_qoe_sessions=('poor_qoe_flag','sum'),avg_latency_ms=('latency_ms','mean'),avg_downlink_mbps=('avg_downlink_mbps','mean'))
    suse=s.groupby(['subscriber_id','month'],as_index=False).agg(sms_count=('sms_id','count'),failed_sms=('failed_sms_flag','sum'))
    usage=su[['subscriber_id','customer_id','plan_id','device_id','home_region']].merge(vu,on='subscriber_id',how='left').merge(du,on=['subscriber_id','month'],how='outer').merge(suse,on=['subscriber_id','month'],how='outer')
    usage=usage.fillna({'voice_minutes':0,'call_count':0,'dropped_calls':0,'roaming_calls':0,'data_gb':0,'data_sessions':0,'poor_qoe_sessions':0,'sms_count':0,'failed_sms':0}); usage=usage.merge(pl[['plan_id','monthly_fee','included_voice_minutes','included_sms','included_data_gb','overage_per_gb']],on='plan_id',how='left'); usage['data_overage_gb']=(usage.data_gb-usage.included_data_gb).clip(lower=0); usage['estimated_data_overage_charge']=usage.data_overage_gb*usage.overage_per_gb; usage['high_usage_flag']=usage.data_gb>=CFG['high_data_usage_gb']; gold('mart_subscriber_monthly_usage',usage)

    cell_health=k.groupby(['cell_id','site_id','technology'],as_index=False).agg(avg_users=('connected_users','mean'),avg_throughput_mbps=('throughput_mbps','mean'),avg_prb_utilization_pct=('prb_utilization_pct','mean'),avg_drop_call_rate_pct=('drop_call_rate_pct','mean'),avg_availability_pct=('availability_pct','mean'),high_util_hours=('high_utilization_flag','sum'),high_drop_hours=('high_drop_rate_flag','sum'))
    cell_calls=v.groupby('cell_id',as_index=False).agg(actual_calls=('call_id','count'),actual_dropped_calls=('dropped_call_flag','sum')); cell_data=d.groupby('cell_id',as_index=False).agg(data_gb=('data_gb','sum'),poor_qoe_sessions=('poor_qoe_flag','sum')); cell_health=cell_health.merge(cell_calls,on='cell_id',how='left').merge(cell_data,on='cell_id',how='left').fillna(0); cell_health['actual_drop_rate']=safe_div(cell_health.actual_dropped_calls,cell_health.actual_calls); gold('mart_cell_network_health',cell_health.sort_values(['high_util_hours','high_drop_hours'],ascending=False))

    site_rel=ou.groupby('site_id',as_index=False).agg(outage_count=('outage_id','count'),outage_hours=('duration_hours','sum'),customers_affected=('customers_affected','sum')).merge(si[['site_id','site_name','city','region']],on='site_id',how='left'); site_rel['availability_estimate_pct']=(1-(site_rel.outage_hours/(61*24)))*100; gold('mart_site_reliability',site_rel.sort_values('availability_estimate_pct'))

    paid=pa[pa.payment_status.eq('Posted')].groupby('invoice_id',as_index=False).agg(amount_paid=('payment_amount','sum'),last_payment_date=('payment_date','max')); ar=inv.merge(paid,on='invoice_id',how='left').fillna({'amount_paid':0}); ar['open_amount']=ar.total_amount-ar.amount_paid; ar['days_past_due']=(pd.Timestamp(CFG['past_due_as_of_date'])-pd.to_datetime(ar.due_date,errors='coerce')).dt.days; ar['past_due_flag']=(ar.open_amount>0)&(ar.days_past_due>0); gold('mart_accounts_receivable',ar)
    revenue=ar.groupby('billing_month',as_index=False).agg(invoiced_amount=('total_amount','sum'),collected_amount=('amount_paid','sum'),open_amount=('open_amount','sum'),invoice_count=('invoice_id','count'),past_due_invoices=('past_due_flag','sum')); revenue['collection_rate']=safe_div(revenue.collected_amount,revenue.invoiced_amount); gold('mart_monthly_revenue',revenue)

    ticket=ti.groupby('customer_id',as_index=False).agg(ticket_count=('ticket_id','count'),open_ticket_count=('resolved_ts',lambda x:x.isna().sum()),avg_resolution_hours=('resolution_hours','mean')); cust_usage=usage.groupby('customer_id',as_index=False).agg(total_data_gb=('data_gb','sum'),dropped_calls=('dropped_calls','sum'),poor_qoe_sessions=('poor_qoe_sessions','sum')); cust_rev=ar.merge(su[['subscriber_id','customer_id']],on='subscriber_id',how='left').groupby('customer_id',as_index=False).agg(billed_amount=('total_amount','sum'),open_amount=('open_amount','sum'),past_due_invoices=('past_due_flag','sum'))
    c360=cu.merge(ticket,on='customer_id',how='left').merge(cust_usage,on='customer_id',how='left').merge(cust_rev,on='customer_id',how='left').fillna({'ticket_count':0,'open_ticket_count':0,'total_data_gb':0,'dropped_calls':0,'poor_qoe_sessions':0,'billed_amount':0,'open_amount':0,'past_due_invoices':0}); c360['churn_risk_score']=c360.ticket_count*2+c360.open_ticket_count*3+c360.dropped_calls*.5+c360.poor_qoe_sessions*.25+c360.past_due_invoices*2+np.where(c360.active_flag==False,20,0); gold('mart_customer_360_churn_risk',c360.sort_values('churn_risk_score',ascending=False))

    planperf=usage.groupby('plan_id',as_index=False).agg(subscriber_months=('subscriber_id','count'),data_gb=('data_gb','sum'),voice_minutes=('voice_minutes','sum'),estimated_overage=('estimated_data_overage_charge','sum'),high_usage_subscriber_months=('high_usage_flag','sum')).merge(pl[['plan_id','plan_name','plan_type','monthly_fee']],on='plan_id',how='left'); planperf['estimated_base_revenue']=planperf.subscriber_months*planperf.monthly_fee; planperf['estimated_total_revenue']=planperf.estimated_base_revenue+planperf.estimated_overage; gold('mart_plan_performance',planperf.sort_values('estimated_total_revenue',ascending=False))

    dev_adopt=de.merge(su[['device_id','subscriber_status']],on='device_id',how='left').groupby(['manufacturer','device_type'],as_index=False).agg(device_count=('device_id','count'),five_g_capable=('5g_capable_flag','sum'),active_subscribers=('subscriber_status',lambda x:(x=='Active').sum())); dev_adopt['five_g_adoption_rate']=safe_div(dev_adopt.five_g_capable,dev_adopt.device_count); gold('mart_device_5g_adoption',dev_adopt)
    supp=ti.groupby(['ticket_category','priority'],as_index=False).agg(ticket_count=('ticket_id','count'),avg_resolution_hours=('resolution_hours','mean'),unresolved_count=('resolved_ts',lambda x:x.isna().sum())); gold('mart_support_operations',supp)

    for n,df in {
        'exception_high_data_usage':usage[usage.high_usage_flag],
        'exception_poor_qoe_sessions':d[d.poor_qoe_flag],
        'exception_dropped_calls':v[v.dropped_call_flag],
        'exception_high_cell_utilization':k[k.high_utilization_flag],
        'exception_network_availability':k[k.poor_availability_flag],
        'exception_past_due_accounts':ar[ar.past_due_flag],
        'exception_high_churn_risk':c360[c360.churn_risk_score>=15],
        'exception_open_critical_alarms':al[(al.severity=='Critical')&(al.alarm_status!='Cleared')]
    }.items(): gold(n,df)

    dq=[]
    for n,c in raw_counts.items(): dq.append({'check_name':f'{n}_raw_to_silver_delta','table_name':n,'issue_count':c-len(D[n]),'severity':'info'})
    for n,c in qs.items(): dq.append({'check_name':n,'table_name':n.split('_')[0],'issue_count':c,'severity':'warning' if c else 'pass'})
    write_csv(pd.DataFrame(dq),REP/'data_quality_report.csv'); write_csv(pd.DataFrame([{'pipeline_name':CFG['project_name'],'run_timestamp_utc':pd.Timestamp.utcnow().isoformat(),'raw_file_count':len(RAW),'raw_total_rows':sum(raw_counts.values()),'silver_total_rows':sum(len(x) for x in D.values()),'quarantine_total_rows':sum(qs.values()),'gold_table_count':len(list(GO.glob('*.csv'))),'status':'SUCCESS'}]),REP/'pipeline_audit_log.csv')
    meta={'last_successful_run_utc':pd.Timestamp.utcnow().isoformat(),'voice_watermark':str(vo.call_start_ts.max()),'data_watermark':str(da.session_start_ts.max()),'kpi_watermark':str(kp.kpi_ts.max()),'ticket_watermark':str(ti.opened_ts.max()),'status':'SUCCESS'}; (META/'run_watermarks.json').write_text(json.dumps(meta,indent=2),encoding='utf-8')
    print(f'Telecom medallion ETL complete. Gold={len(list(GO.glob("*.csv")))} Quarantine={len(list(Q.glob("*.csv")))}')
if __name__=='__main__': main()
