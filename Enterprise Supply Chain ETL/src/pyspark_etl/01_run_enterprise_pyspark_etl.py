from pathlib import Path
from pyspark.sql import SparkSession, functions as F
ROOT=Path(__file__).resolve().parents[2]; RAW=ROOT/'data/raw'; SILVER=ROOT/'data/silver/pyspark'; GOLD=ROOT/'data/gold/pyspark'; QUAR=ROOT/'data/quarantine/pyspark'; REPORT=ROOT/'reports/pyspark'
def spark_session(): return SparkSession.builder.appName('EnterpriseSupplyChainETL').master('local[*]').getOrCreate()
def read(s,rel): return s.read.option('header',True).option('inferSchema',False).csv(str(RAW/rel))
def write(df,path): df.coalesce(1).write.mode('overwrite').option('header',True).csv(str(path))
def txt(c,default='Unknown',case=None):
    v=F.trim(F.coalesce(F.col(c),F.lit(''))); v=F.when(v=='',F.lit(default)).otherwise(v)
    return F.upper(v) if case=='upper' else F.lower(v) if case=='lower' else F.initcap(v) if case=='title' else v
def num(c,default=None):
    v=F.regexp_replace(F.regexp_replace(F.trim(F.coalesce(F.col(c),F.lit(''))),'\\$',''),',','').cast('double')
    return F.coalesce(v,F.lit(float(default))) if default is not None else v
def dt(c):
    v=F.trim(F.coalesce(F.col(c),F.lit(''))); return F.coalesce(F.to_date(v,'yyyy-MM-dd'),F.to_date(v,'MM/dd/yyyy'),F.to_date(v,'yyyy/MM/dd'))
def ts(c):
    v=F.trim(F.coalesce(F.col(c),F.lit(''))); return F.coalesce(F.to_timestamp(v,'yyyy-MM-dd HH:mm:ss'),F.to_timestamp(v,'MM/dd/yyyy HH:mm'))
def main():
    s=spark_session()
    customers=read(s,'master/customers.csv').withColumn('customer_id',txt('customer_id','','upper')).withColumn('customer_name',txt('customer_name',case='title')).withColumn('customer_segment',txt('customer_segment','Unknown','title')).dropDuplicates(['customer_id'])
    products=read(s,'master/products.csv').withColumn('product_id',txt('product_id','','upper')).withColumn('category',txt('category',case='title')).withColumn('subcategory',txt('subcategory',case='title')).withColumn('supplier_id',txt('supplier_id','','upper')).withColumn('standard_cost',num('standard_cost')).withColumn('list_price',num('list_price')).dropDuplicates(['product_id'])
    warehouses=read(s,'master/warehouses.csv').withColumn('warehouse_id',txt('warehouse_id','','upper')).dropDuplicates(['warehouse_id'])
    fx=read(s,'reference/fx_rates.csv').withColumn('rate_date',dt('rate_date')).withColumn('currency',txt('currency','USD','upper')).withColumn('usd_rate',num('usd_rate')).dropDuplicates(['rate_date','currency'])
    orders=read(s,'transactions/sales_orders.csv').withColumn('order_id',txt('order_id','','upper')).withColumn('customer_id',txt('customer_id','','upper')).withColumn('order_date',dt('order_date')).withColumn('sales_channel',txt('sales_channel',case='title')).withColumn('currency',txt('currency','USD','upper')).withColumn('ship_from_warehouse_id',txt('ship_from_warehouse_id','','upper')).withColumn('created_ts',ts('created_ts')).dropDuplicates(['order_id'])
    lines=read(s,'transactions/sales_order_lines.csv').withColumn('order_line_id',txt('order_line_id','','upper')).withColumn('order_id',txt('order_id','','upper')).withColumn('product_id',txt('product_id','','upper')).withColumn('ordered_qty',num('ordered_qty')).withColumn('unit_price',num('unit_price')).withColumn('discount_pct',num('discount_pct',0)).withColumn('valid_values',(F.col('ordered_qty')>0)&F.col('unit_price').isNotNull()&F.col('discount_pct').between(0,100)).dropDuplicates(['order_line_id'])
    shipments=read(s,'transactions/shipments.csv').withColumn('shipment_id',txt('shipment_id','','upper')).withColumn('order_id',txt('order_id','','upper')).withColumn('carrier_id',txt('carrier_id','','upper')).withColumn('ship_ts',ts('ship_ts')).withColumn('delivery_ts',ts('delivery_ts')).dropDuplicates(['shipment_id'])
    for name,df in {'customers':customers,'products':products,'warehouses':warehouses,'fx_rates':fx,'sales_orders':orders,'sales_order_lines':lines,'shipments':shipments}.items(): write(df,SILVER/name)
    bad_customers=orders.join(customers.select('customer_id'),'customer_id','left_anti'); bad_products=lines.join(products.select('product_id'),'product_id','left_anti'); bad_values=lines.filter(~F.col('valid_values'))
    write(bad_customers,QUAR/'sales_orders_invalid_customer_id'); write(bad_products,QUAR/'sales_order_lines_invalid_product_id'); write(bad_values,QUAR/'sales_order_lines_bad_values')
    sales=lines.filter('valid_values').join(orders.select('order_id','customer_id','order_date','sales_channel','currency','ship_from_warehouse_id'),'order_id','inner').join(products.select('product_id','category','subcategory','standard_cost'),'product_id','inner').join(fx,(F.col('order_date')==fx.rate_date)&(F.col('currency')==fx.currency),'left').withColumn('usd_rate',F.coalesce(F.col('usd_rate'),F.lit(1.0))).withColumn('gross_sales',F.col('ordered_qty')*F.col('unit_price')).withColumn('discount_amount',F.col('gross_sales')*(F.col('discount_pct')/100)).withColumn('net_sales',F.col('gross_sales')-F.col('discount_amount')).withColumn('net_sales_usd',F.col('net_sales')*F.col('usd_rate')).withColumn('estimated_cost_usd',F.col('ordered_qty')*F.coalesce(F.col('standard_cost'),F.lit(0))*F.col('usd_rate')).withColumn('gross_margin_usd',F.col('net_sales_usd')-F.col('estimated_cost_usd'))
    daily=sales.groupBy('order_date','sales_channel').agg(F.countDistinct('order_id').alias('order_count'),F.count('order_line_id').alias('line_count'),F.sum('ordered_qty').alias('units_ordered'),F.sum('net_sales_usd').alias('net_sales_usd'),F.sum('gross_margin_usd').alias('gross_margin_usd'))
    prod=sales.groupBy('product_id','category','subcategory').agg(F.sum('ordered_qty').alias('units_ordered'),F.sum('net_sales_usd').alias('net_sales_usd'),F.sum('gross_margin_usd').alias('gross_margin_usd'),F.countDistinct('order_id').alias('order_count')).orderBy(F.desc('net_sales_usd'))
    write(sales,GOLD/'fact_sales_order_lines'); write(daily,GOLD/'mart_daily_sales'); write(prod,GOLD/'mart_product_performance')
    dq=s.createDataFrame([('sales_orders_invalid_customer_id',bad_customers.count()),('sales_order_lines_invalid_product_id',bad_products.count()),('sales_order_lines_bad_values',bad_values.count())],['check_name','issue_count']); write(dq,REPORT/'data_quality_report')
    print('PySpark enterprise supply chain ETL complete.'); s.stop()
if __name__=='__main__': main()
