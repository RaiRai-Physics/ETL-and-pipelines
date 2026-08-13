from pathlib import Path
from pyspark.sql import SparkSession, functions as F

ROOT=Path(__file__).resolve().parents[2]
RAW=ROOT/"data/raw"; SILVER=ROOT/"data/silver/pyspark"; GOLD=ROOT/"data/gold/pyspark"; Q=ROOT/"data/quarantine/pyspark"

def spark():
    return SparkSession.builder.appName("WalmartStyleRetailETL").master("local[*]").getOrCreate()

def read(s,rel): return s.read.option("header",True).option("inferSchema",False).csv(str(RAW/rel))
def write(df,path): df.coalesce(1).write.mode("overwrite").option("header",True).csv(str(path))
def txt(c,default="Unknown",case=None):
    v=F.trim(F.coalesce(F.col(c),F.lit(""))); v=F.when(v=="",default).otherwise(v)
    return F.upper(v) if case=="upper" else F.lower(v) if case=="lower" else F.initcap(v) if case=="title" else v
def num(c,default=None):
    n=F.regexp_replace(F.regexp_replace(F.trim(F.coalesce(F.col(c),F.lit(""))),"\\$",""),",","").cast("double")
    return F.coalesce(n,F.lit(float(default))) if default is not None else n
def ts(c):
    v=F.trim(F.coalesce(F.col(c),F.lit("")))
    return F.coalesce(F.to_timestamp(v,"yyyy-MM-dd HH:mm:ss"),F.to_timestamp(v,"MM/dd/yyyy HH:mm"))
def dt(c):
    v=F.trim(F.coalesce(F.col(c),F.lit("")))
    return F.coalesce(F.to_date(v,"yyyy-MM-dd"),F.to_date(v,"MM/dd/yyyy"))

def main():
    s=spark()
    stores=read(s,"master/stores.csv").withColumn("store_id",txt("store_id","", "upper")).withColumn("region",txt("region",case="title")).dropDuplicates(["store_id"])
    products=(read(s,"master/products.csv").withColumn("product_id",txt("product_id","", "upper")).withColumn("department",txt("department",case="title"))
              .withColumn("subcategory",txt("subcategory",case="title")).withColumn("supplier_id",txt("supplier_id","", "upper"))
              .withColumn("standard_cost",num("standard_cost")).withColumn("regular_price",num("regular_price")).dropDuplicates(["product_id"]))
    pos=(read(s,"transactions/pos_transactions.csv").withColumn("transaction_id",txt("transaction_id","", "upper")).withColumn("store_id",txt("store_id","", "upper"))
         .withColumn("customer_id",txt("customer_id","", "upper")).withColumn("transaction_ts",ts("transaction_ts")).withColumn("transaction_date",F.to_date("transaction_ts"))
         .withColumn("transaction_type",txt("transaction_type",case="upper")).withColumn("receipt_status",txt("receipt_status",case="title")).dropDuplicates(["transaction_id"]))
    lines=(read(s,"transactions/pos_transaction_lines.csv").withColumn("transaction_line_id",txt("transaction_line_id","", "upper")).withColumn("transaction_id",txt("transaction_id","", "upper"))
           .withColumn("product_id",txt("product_id","", "upper")).withColumn("quantity",num("quantity")).withColumn("unit_price",num("unit_price"))
           .withColumn("regular_price",num("regular_price")).withColumn("coupon_amount",num("coupon_amount",0)).withColumn("tax_amount",num("tax_amount",0))
           .withColumn("line_status",txt("line_status",case="title")).withColumn("valid_values",(F.col("quantity")>0)&F.col("unit_price").isNotNull()).dropDuplicates(["transaction_line_id"]))
    inv=(read(s,"inventory/inventory_snapshots.csv").withColumn("snapshot_date",dt("snapshot_date")).withColumn("store_id",txt("store_id","", "upper")).withColumn("product_id",txt("product_id","", "upper"))
         .withColumn("on_hand_qty",num("on_hand_qty")).withColumn("reserved_qty",num("reserved_qty",0)).withColumn("on_order_qty",num("on_order_qty",0))
         .withColumn("reorder_point",num("reorder_point",0)).withColumn("available_qty",F.col("on_hand_qty")-F.col("reserved_qty"))
         .withColumn("valid_values",F.col("on_hand_qty").isNotNull()&(F.col("on_hand_qty")>=0)).dropDuplicates(["snapshot_date","store_id","product_id"]))
    for n,df in {"stores":stores,"products":products,"pos_transactions":pos,"pos_transaction_lines":lines,"inventory_snapshots":inv}.items(): write(df,SILVER/n)

    bad_lines=lines.filter(~F.col("valid_values"))
    invalid_products=lines.join(products.select("product_id"),"product_id","left_anti")
    invalid_stores=pos.join(stores.select("store_id"),"store_id","left_anti")
    write(bad_lines,Q/"lines_bad_values"); write(invalid_products,Q/"lines_invalid_product"); write(invalid_stores,Q/"pos_invalid_store")

    sales=(lines.filter(F.col("valid_values")).join(pos,"transaction_id","inner").join(products.select("product_id","department","subcategory","standard_cost"),"product_id","inner")
           .filter((F.col("transaction_type")=="SALE")&(F.col("receipt_status")=="Completed")&(F.col("line_status")=="Sold"))
           .withColumn("gross_sales",F.col("quantity")*F.col("unit_price")).withColumn("net_sales",F.col("gross_sales")-F.col("coupon_amount"))
           .withColumn("estimated_cogs",F.col("quantity")*F.coalesce(F.col("standard_cost"),F.lit(0))).withColumn("gross_margin",F.col("net_sales")-F.col("estimated_cogs")))
    daily=(sales.groupBy("transaction_date","store_id","department").agg(F.countDistinct("transaction_id").alias("transaction_count"),F.sum("quantity").alias("units"),
           F.sum("net_sales").alias("net_sales"),F.sum("gross_margin").alias("gross_margin")))
    latest_date=inv.agg(F.max("snapshot_date").alias("d")).collect()[0]["d"]
    latest=inv.filter((F.col("snapshot_date")==F.lit(latest_date))&F.col("valid_values")).withColumn("stockout_flag",F.col("available_qty")<=0).withColumn("low_stock_flag",F.col("available_qty")<=F.col("reorder_point"))
    write(sales,GOLD/"fact_pos_sales_lines"); write(daily,GOLD/"mart_daily_store_sales"); write(latest,GOLD/"inventory_position_current")
    dq=s.createDataFrame([("lines_bad_values",bad_lines.count()),("lines_invalid_product",invalid_products.count()),("pos_invalid_store",invalid_stores.count())],["check_name","issue_count"])
    write(dq,ROOT/"reports/pyspark/data_quality_report")
    print("PySpark retail ETL complete."); s.stop()

if __name__=="__main__": main()
