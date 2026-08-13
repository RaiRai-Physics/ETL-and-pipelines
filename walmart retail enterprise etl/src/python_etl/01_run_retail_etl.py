from pathlib import Path
import sys,json
import pandas as pd
import numpy as np
ROOT=Path(__file__).resolve().parents[2]; sys.path.append(str(ROOT))
from src.common.utils import load_config,read_csv,write_csv,clean_text,parse_date,parse_ts,parse_num,parse_bool,stable_hash,safe_div

CFG=load_config()
BRONZE=ROOT/CFG["bronze_path"]; SILVER=ROOT/CFG["silver_path"]; Q=ROOT/CFG["quarantine_path"]; GOLD=ROOT/CFG["gold_path"]; REPORT=ROOT/CFG["report_path"]; META=ROOT/CFG["metadata_path"]
RAW={
"stores":"data/raw/master/stores.csv","suppliers":"data/raw/master/suppliers.csv","products":"data/raw/master/products.csv","customers":"data/raw/master/customers.csv",
"associates":"data/raw/master/associates.csv","promotions":"data/raw/master/promotions.csv","calendar":"data/raw/reference/calendar.csv",
"pos_transactions":"data/raw/transactions/pos_transactions.csv","pos_transaction_lines":"data/raw/transactions/pos_transaction_lines.csv",
"payments":"data/raw/transactions/payments.csv","online_orders":"data/raw/transactions/online_orders.csv","online_order_lines":"data/raw/transactions/online_order_lines.csv",
"shipments":"data/raw/transactions/shipments.csv","returns":"data/raw/transactions/returns.csv","inventory_snapshots":"data/raw/inventory/inventory_snapshots.csv",
"inventory_adjustments":"data/raw/inventory/inventory_adjustments.csv","price_cdc":"data/raw/cdc/price_cdc.csv","customer_cdc":"data/raw/cdc/customer_cdc.csv"}

def raw(name): return read_csv(ROOT/RAW[name])
def bronze(name,df):
    x=df.copy(); x["_loaded_at_utc"]=pd.Timestamp.utcnow().isoformat(); x["_source_file"]=RAW[name]; write_csv(x,BRONZE/f"{name}.csv")
def silver(name,df): write_csv(df,SILVER/f"{name}.csv")
def gold(name,df): write_csv(df,GOLD/f"{name}.csv")
def quarantine(name,df,reason):
    x=df.copy(); x["quarantine_reason"]=reason
    if len(x)==0: x=pd.DataFrame(columns=["quarantine_reason"])
    write_csv(x,Q/f"{name}.csv")

def c_stores(df):
    x=df.copy(); x["store_id"]=clean_text(x["store_id"],"", "upper"); x["store_name"]=clean_text(x["store_name"],case="title"); x["store_format"]=clean_text(x["store_format"],case="title")
    x["city"]=clean_text(x["city"],case="title"); x["state"]=clean_text(x["state"],case="upper"); x["region"]=clean_text(x["region"],case="title")
    x["open_date"]=parse_date(x["open_date"]); x["square_feet"]=parse_num(x["square_feet"],0); x["active_flag"]=parse_bool(x["active_flag"])
    return x.drop_duplicates("store_id")

def c_suppliers(df):
    x=df.copy(); x["supplier_id"]=clean_text(x["supplier_id"],"", "upper"); x["supplier_name"]=clean_text(x["supplier_name"],case="title")
    x["country"]=clean_text(x["country"],case="upper"); x["supplier_tier"]=clean_text(x["supplier_tier"],case="title"); x["lead_time_days"]=parse_num(x["lead_time_days"])
    x["on_time_rate"]=parse_num(x["on_time_rate"]); x["defect_rate"]=parse_num(x["defect_rate"]); x["active_flag"]=parse_bool(x["active_flag"])
    return x.drop_duplicates("supplier_id")

def c_products(df):
    x=df.copy(); x["product_id"]=clean_text(x["product_id"],"", "upper"); x["product_name"]=clean_text(x["product_name"],case="title")
    x["department"]=clean_text(x["department"],case="title"); x["subcategory"]=clean_text(x["subcategory"],case="title"); x["brand"]=clean_text(x["brand"],case="title")
    x["supplier_id"]=clean_text(x["supplier_id"],"", "upper"); x["standard_cost"]=parse_num(x["standard_cost"]); x["regular_price"]=parse_num(x["regular_price"])
    x["tax_category"]=clean_text(x["tax_category"],case="title"); x["product_status"]=clean_text(x["product_status"],case="title"); x["launch_date"]=parse_date(x["launch_date"])
    return x.drop_duplicates("product_id")

def c_customers(df):
    x=df.copy(); x["customer_id"]=clean_text(x["customer_id"],"", "upper"); x["customer_name"]=clean_text(x["customer_name"],case="title")
    x["email"]=clean_text(x["email"],"unknown@example.com","lower"); x["city"]=clean_text(x["city"],case="title"); x["state"]=clean_text(x["state"],case="upper"); x["region"]=clean_text(x["region"],case="title")
    x["signup_date"]=parse_date(x["signup_date"]); x["loyalty_tier"]=clean_text(x["loyalty_tier"],"Unknown","title"); x["household_size"]=parse_num(x["household_size"]); x["active_flag"]=parse_bool(x["active_flag"])
    x["customer_hash"]=x["customer_id"].map(stable_hash); return x.drop_duplicates("customer_id")

def c_associates(df):
    x=df.copy(); x["associate_id"]=clean_text(x["associate_id"],"", "upper"); x["store_id"]=clean_text(x["store_id"],"", "upper"); x["role"]=clean_text(x["role"],case="title")
    x["hire_date"]=parse_date(x["hire_date"]); x["employment_status"]=clean_text(x["employment_status"],case="title"); return x.drop_duplicates("associate_id")

def c_promotions(df):
    x=df.copy(); x["promotion_id"]=clean_text(x["promotion_id"],"", "upper"); x["promotion_name"]=clean_text(x["promotion_name"],case="title"); x["promotion_type"]=clean_text(x["promotion_type"],case="title")
    x["discount_value"]=parse_num(x["discount_value"],0); x["start_date"]=parse_date(x["start_date"]); x["end_date"]=parse_date(x["end_date"]); x["channel"]=clean_text(x["channel"],case="title"); x["active_flag"]=parse_bool(x["active_flag"])
    return x.drop_duplicates("promotion_id")

def c_calendar(df):
    x=df.copy(); x["calendar_date"]=parse_date(x["calendar_date"]); return x.drop_duplicates("date_key")

def c_pos(df):
    x=df.copy(); x["transaction_id"]=clean_text(x["transaction_id"],"", "upper"); x["store_id"]=clean_text(x["store_id"],"", "upper"); x["associate_id"]=clean_text(x["associate_id"],"", "upper")
    x["customer_id"]=clean_text(x["customer_id"],"", "upper"); x["transaction_ts"]=parse_ts(x["transaction_ts"]); x["transaction_date"]=x["transaction_ts"].dt.date.astype("string")
    x["transaction_type"]=clean_text(x["transaction_type"],case="upper"); x["sales_channel"]=clean_text(x["sales_channel"],case="title"); x["receipt_status"]=clean_text(x["receipt_status"],case="title"); x["batch_id"]=clean_text(x["batch_id"],"UNKNOWN","upper")
    return x.drop_duplicates("transaction_id")

def c_lines(df):
    x=df.copy(); x["transaction_line_id"]=clean_text(x["transaction_line_id"],"", "upper"); x["transaction_id"]=clean_text(x["transaction_id"],"", "upper"); x["product_id"]=clean_text(x["product_id"],"", "upper")
    x["line_number"]=parse_num(x["line_number"],0); x["quantity"]=parse_num(x["quantity"]); x["unit_price"]=parse_num(x["unit_price"]); x["regular_price"]=parse_num(x["regular_price"])
    x["promotion_id"]=clean_text(x["promotion_id"],"", "upper"); x["coupon_amount"]=parse_num(x["coupon_amount"],0); x["tax_amount"]=parse_num(x["tax_amount"],0); x["line_status"]=clean_text(x["line_status"],case="title")
    x["valid_values"]=x["quantity"].notna()&(x["quantity"]>0)&x["unit_price"].notna()&(x["unit_price"]>=0); return x.drop_duplicates("transaction_line_id")

def c_payments(df):
    x=df.copy(); x["payment_id"]=clean_text(x["payment_id"],"", "upper"); x["transaction_id"]=clean_text(x["transaction_id"],"", "upper"); x["payment_method"]=clean_text(x["payment_method"],case="title").replace({"Credit Card":"Credit Card"})
    x["payment_status"]=clean_text(x["payment_status"],case="title"); x["payment_amount"]=parse_num(x["payment_amount"]); return x.drop_duplicates("payment_id")

def c_online(df):
    x=df.copy(); x["online_order_id"]=clean_text(x["online_order_id"],"", "upper"); x["customer_id"]=clean_text(x["customer_id"],"", "upper"); x["order_ts"]=parse_ts(x["order_ts"]); x["order_date"]=x["order_ts"].dt.date.astype("string")
    x["fulfillment_type"]=clean_text(x["fulfillment_type"],case="title"); x["fulfillment_store_id"]=clean_text(x["fulfillment_store_id"],"", "upper"); x["order_status"]=clean_text(x["order_status"],case="title"); x["device_type"]=clean_text(x["device_type"],case="title")
    return x.drop_duplicates("online_order_id")

def c_olines(df):
    x=df.copy(); x["online_order_line_id"]=clean_text(x["online_order_line_id"],"", "upper"); x["online_order_id"]=clean_text(x["online_order_id"],"", "upper"); x["product_id"]=clean_text(x["product_id"],"", "upper")
    x["quantity"]=parse_num(x["quantity"]); x["unit_price"]=parse_num(x["unit_price"]); x["discount_amount"]=parse_num(x["discount_amount"],0); x["line_status"]=clean_text(x["line_status"],case="title")
    x["valid_values"]=x["quantity"].notna()&(x["quantity"]>0)&x["unit_price"].notna()&(x["unit_price"]>=0); return x.drop_duplicates("online_order_line_id")

def c_ship(df):
    x=df.copy(); x["shipment_id"]=clean_text(x["shipment_id"],"", "upper"); x["online_order_id"]=clean_text(x["online_order_id"],"", "upper"); x["carrier"]=clean_text(x["carrier"],case="title")
    x["ship_ts"]=parse_ts(x["ship_ts"]); x["delivery_ts"]=parse_ts(x["delivery_ts"]); x["shipping_cost"]=parse_num(x["shipping_cost"],0); x["shipment_status"]=clean_text(x["shipment_status"],case="title")
    x["delivery_days"]=(x["delivery_ts"]-x["ship_ts"]).dt.total_seconds()/86400; x["late_delivery_flag"]=(x["delivery_days"]>5)|x["delivery_ts"].isna()|(x["shipment_status"]=="Delayed")
    return x.drop_duplicates("shipment_id")

def c_returns(df):
    x=df.copy(); x["return_id"]=clean_text(x["return_id"],"", "upper"); x["original_transaction_id"]=clean_text(x["original_transaction_id"],"", "upper"); x["transaction_line_id"]=clean_text(x["transaction_line_id"],"", "upper")
    x["store_id"]=clean_text(x["store_id"],"", "upper"); x["product_id"]=clean_text(x["product_id"],"", "upper"); x["return_date"]=parse_date(x["return_date"]); x["return_qty"]=parse_num(x["return_qty"]); x["refund_amount"]=parse_num(x["refund_amount"],0)
    x["return_reason"]=clean_text(x["return_reason"],case="title"); x["return_status"]=clean_text(x["return_status"],case="title"); return x.drop_duplicates("return_id")

def c_inv(df):
    x=df.copy(); x["snapshot_date"]=parse_date(x["snapshot_date"]); x["store_id"]=clean_text(x["store_id"],"", "upper"); x["product_id"]=clean_text(x["product_id"],"", "upper")
    for c in ["on_hand_qty","reserved_qty","on_order_qty","reorder_point","safety_stock_qty"]: x[c]=parse_num(x[c])
    x["inventory_status"]=clean_text(x["inventory_status"],case="title"); x["available_qty"]=x["on_hand_qty"]-x["reserved_qty"]; x["valid_values"]=x["on_hand_qty"].notna()&(x["on_hand_qty"]>=0)&x["reserved_qty"].notna()
    return x.drop_duplicates(["snapshot_date","store_id","product_id"],keep="last")

def c_adj(df):
    x=df.copy(); x["adjustment_id"]=clean_text(x["adjustment_id"],"", "upper"); x["store_id"]=clean_text(x["store_id"],"", "upper"); x["product_id"]=clean_text(x["product_id"],"", "upper"); x["adjustment_ts"]=parse_ts(x["adjustment_ts"])
    x["adjustment_qty"]=parse_num(x["adjustment_qty"]); x["adjustment_reason"]=clean_text(x["adjustment_reason"],case="title"); x["approved_by"]=clean_text(x["approved_by"],"Unapproved","lower"); return x.drop_duplicates("adjustment_id")

def c_price_cdc(df):
    x=df.copy(); x["cdc_id"]=clean_text(x["cdc_id"],"", "upper"); x["product_id"]=clean_text(x["product_id"],"", "upper"); x["effective_ts"]=parse_ts(x["effective_ts"]); x["effective_date"]=x["effective_ts"].dt.date.astype("string")
    x["operation"]=clean_text(x["operation"],case="upper"); x["new_regular_price"]=parse_num(x["new_regular_price"]); x["new_product_status"]=clean_text(x["new_product_status"],case="title"); return x.drop_duplicates("cdc_id")

def c_customer_cdc(df):
    x=df.copy(); x["cdc_id"]=clean_text(x["cdc_id"],"", "upper"); x["customer_id"]=clean_text(x["customer_id"],"", "upper"); x["effective_ts"]=parse_ts(x["effective_ts"]); x["effective_date"]=x["effective_ts"].dt.date.astype("string")
    x["operation"]=clean_text(x["operation"],case="upper"); x["new_loyalty_tier"]=clean_text(x["new_loyalty_tier"],case="title"); x["new_active_flag"]=parse_bool(x["new_active_flag"]); return x.drop_duplicates("cdc_id")

CLEAN={"stores":c_stores,"suppliers":c_suppliers,"products":c_products,"customers":c_customers,"associates":c_associates,"promotions":c_promotions,"calendar":c_calendar,
"pos_transactions":c_pos,"pos_transaction_lines":c_lines,"payments":c_payments,"online_orders":c_online,"online_order_lines":c_olines,"shipments":c_ship,
"returns":c_returns,"inventory_snapshots":c_inv,"inventory_adjustments":c_adj,"price_cdc":c_price_cdc,"customer_cdc":c_customer_cdc}

def customer_scd2(customers,cdc):
    base=customers[["customer_id","customer_name","email","city","state","region","signup_date","loyalty_tier","household_size","active_flag"]].copy()
    base["effective_start_date"]=base["signup_date"].fillna("1900-01-01"); base["source"]="customer_master"
    upd=cdc.merge(customers[["customer_id","customer_name","email","city","state","region","signup_date","household_size"]],on="customer_id",how="inner")
    upd=upd.rename(columns={"new_loyalty_tier":"loyalty_tier","new_active_flag":"active_flag","effective_date":"effective_start_date","change_reason":"source"})
    upd=upd[base.columns]; x=pd.concat([base,upd],ignore_index=True); x["_d"]=pd.to_datetime(x["effective_start_date"],errors="coerce"); x=x.sort_values(["customer_id","_d"])
    x["effective_end_date"]=x.groupby("customer_id")["_d"].shift(-1).dt.date.astype("string").fillna("9999-12-31"); x["is_current"]=x["effective_end_date"].eq("9999-12-31")
    x["customer_sk"]=[stable_hash(a,b,c) for a,b,c in zip(x["customer_id"],x["effective_start_date"],x["loyalty_tier"])]
    return x.drop(columns="_d")

def product_scd2(products,cdc):
    base=products.copy(); base["effective_start_date"]=base["launch_date"].fillna("1900-01-01"); base["source"]="product_master"
    upd=cdc.merge(products.drop(columns=["regular_price","product_status"]),on="product_id",how="inner"); upd["regular_price"]=upd["new_regular_price"]; upd["product_status"]=upd["new_product_status"]; upd["effective_start_date"]=upd["effective_date"]; upd["source"]="price_cdc"
    upd=upd[base.columns]; x=pd.concat([base,upd],ignore_index=True); x["_d"]=pd.to_datetime(x["effective_start_date"],errors="coerce"); x=x.sort_values(["product_id","_d"])
    x["effective_end_date"]=x.groupby("product_id")["_d"].shift(-1).dt.date.astype("string").fillna("9999-12-31"); x["is_current"]=x["effective_end_date"].eq("9999-12-31")
    x["product_sk"]=[stable_hash(a,b,c) for a,b,c in zip(x["product_id"],x["effective_start_date"],x["regular_price"])]
    return x.drop(columns="_d")

def main():
    for d in [BRONZE,SILVER,Q,GOLD,REPORT,META]: d.mkdir(parents=True,exist_ok=True)
    data={}; raw_counts={}
    for name in RAW:
        r=raw(name); raw_counts[name]=len(r); bronze(name,r); data[name]=CLEAN[name](r); silver(name,data[name])

    stores,suppliers,products,customers,associates=data["stores"],data["suppliers"],data["products"],data["customers"],data["associates"]
    pos,lines,payments=data["pos_transactions"],data["pos_transaction_lines"],data["payments"]; online,olines,ship=data["online_orders"],data["online_order_lines"],data["shipments"]
    returns,inv,adj=data["returns"],data["inventory_snapshots"],data["inventory_adjustments"]

    qs={}
    def q(name,df,reason): quarantine(name,df,reason); qs[name]=len(df)
    q("products_invalid_supplier",products[~products["supplier_id"].isin(suppliers["supplier_id"])],"Missing supplier")
    q("associates_invalid_store",associates[~associates["store_id"].isin(stores["store_id"])],"Missing store")
    q("pos_invalid_store",pos[~pos["store_id"].isin(stores["store_id"])],"Missing store")
    q("pos_invalid_associate",pos[~pos["associate_id"].isin(associates["associate_id"])],"Missing associate")
    q("pos_bad_timestamp",pos[pos["transaction_ts"].isna()],"Bad transaction timestamp")
    q("lines_invalid_transaction",lines[~lines["transaction_id"].isin(pos["transaction_id"])],"Missing POS transaction")
    q("lines_invalid_product",lines[~lines["product_id"].isin(products["product_id"])],"Missing product")
    q("lines_bad_values",lines[~lines["valid_values"]],"Bad quantity or unit price")
    q("payments_invalid_transaction",payments[~payments["transaction_id"].isin(pos["transaction_id"])],"Missing transaction")
    q("online_invalid_customer",online[~online["customer_id"].isin(customers["customer_id"])],"Missing customer")
    q("online_invalid_store",online[~online["fulfillment_store_id"].isin(stores["store_id"])],"Missing fulfillment store")
    q("online_lines_invalid_order",olines[~olines["online_order_id"].isin(online["online_order_id"])],"Missing online order")
    q("online_lines_invalid_product",olines[~olines["product_id"].isin(products["product_id"])],"Missing product")
    q("online_lines_bad_values",olines[~olines["valid_values"]],"Bad online line quantity or price")
    q("shipments_invalid_order",ship[~ship["online_order_id"].isin(online["online_order_id"])],"Missing online order")
    q("returns_invalid_transaction",returns[~returns["original_transaction_id"].isin(pos["transaction_id"])],"Missing original transaction")
    q("returns_invalid_line",returns[~returns["transaction_line_id"].isin(lines["transaction_line_id"])],"Missing original line")
    q("inventory_invalid_store",inv[~inv["store_id"].isin(stores["store_id"])],"Missing store")
    q("inventory_invalid_product",inv[~inv["product_id"].isin(products["product_id"])],"Missing product")
    q("inventory_bad_values",inv[~inv["valid_values"]],"Bad inventory quantities")

    cscd=customer_scd2(customers,data["customer_cdc"]); pscd=product_scd2(products,data["price_cdc"])
    gold("dim_customer_scd2",cscd); gold("dim_customer_current",cscd[cscd["is_current"]]); gold("dim_product_scd2",pscd); gold("dim_product_current",pscd[pscd["is_current"]])
    for name,df in {"dim_store":stores,"dim_supplier":suppliers,"dim_associate":associates,"dim_promotion":data["promotions"]}.items(): gold(name,df)

    vl=lines[lines["transaction_id"].isin(pos["transaction_id"])&lines["product_id"].isin(products["product_id"])&lines["valid_values"]].copy()
    sales=vl.merge(pos[["transaction_id","store_id","associate_id","customer_id","transaction_ts","transaction_date","transaction_type","sales_channel","receipt_status"]],on="transaction_id",how="inner").merge(products[["product_id","department","subcategory","brand","standard_cost"]],on="product_id",how="left")
    sales=sales[(sales["transaction_type"]=="SALE")&(sales["receipt_status"]=="Completed")&(sales["line_status"]=="Sold")].copy()
    sales["gross_sales"]=sales["quantity"]*sales["unit_price"]; sales["net_sales"]=sales["gross_sales"]-sales["coupon_amount"]; sales["estimated_cogs"]=sales["quantity"]*sales["standard_cost"].fillna(0); sales["gross_margin"]=sales["net_sales"]-sales["estimated_cogs"]
    sales["markdown_amount"]=(sales["regular_price"]-sales["unit_price"]).clip(lower=0)*sales["quantity"]; gold("fact_pos_sales_lines",sales)

    baskets=sales.groupby("transaction_id",as_index=False).agg(basket_sales=("net_sales","sum"),basket_margin=("gross_margin","sum"),units=("quantity","sum"),sku_count=("product_id","nunique")).merge(pos,on="transaction_id",how="left")
    baskets["high_value_basket_flag"]=baskets["basket_sales"]>=CFG["high_value_basket_threshold"]; gold("fact_pos_transactions",baskets)

    valid_ol=olines[olines["online_order_id"].isin(online["online_order_id"])&olines["product_id"].isin(products["product_id"])&olines["valid_values"]].copy()
    osales=valid_ol.merge(online,on="online_order_id",how="inner").merge(products[["product_id","department","subcategory","brand","standard_cost"]],on="product_id",how="left")
    osales=osales[~osales["order_status"].isin(["Cancelled"])].copy(); osales["gross_sales"]=osales["quantity"]*osales["unit_price"]; osales["net_sales"]=osales["gross_sales"]-osales["discount_amount"]; osales["estimated_cogs"]=osales["quantity"]*osales["standard_cost"].fillna(0); osales["gross_margin"]=osales["net_sales"]-osales["estimated_cogs"]; gold("fact_online_sales_lines",osales)

    gold("fact_returns",returns[returns["original_transaction_id"].isin(pos["transaction_id"])])
    gold("fact_inventory_adjustments",adj[adj["store_id"].isin(stores["store_id"])&adj["product_id"].isin(products["product_id"])])

    paid=payments[payments["payment_status"]=="Approved"].groupby("transaction_id",as_index=False).agg(payment_amount=("payment_amount","sum"),tender_count=("payment_id","count"))
    recon=baskets[["transaction_id","basket_sales"]].merge(paid,on="transaction_id",how="left").fillna({"payment_amount":0,"tender_count":0}); recon["difference"]=recon["payment_amount"]-recon["basket_sales"]; recon["reconciliation_status"]=np.where(recon["difference"].abs()<=CFG["payment_match_tolerance"],"Matched","Mismatch"); gold("recon_pos_sales_vs_payments",recon)

    latest=inv.sort_values("snapshot_date").groupby(["store_id","product_id"],as_index=False).tail(1)
    latest=latest[latest["store_id"].isin(stores["store_id"])&latest["product_id"].isin(products["product_id"])&latest["valid_values"]].copy()
    latest["stockout_flag"]=latest["available_qty"]<=0; latest["low_stock_flag"]=latest["available_qty"]<=latest["reorder_point"]; latest["overstock_flag"]=latest["available_qty"]>latest["reorder_point"]*4; gold("inventory_position_current",latest)

    daily=sales.groupby(["transaction_date","store_id","department"],as_index=False).agg(transaction_count=("transaction_id","nunique"),units=("quantity","sum"),net_sales=("net_sales","sum"),gross_margin=("gross_margin","sum"),markdown_amount=("markdown_amount","sum")); gold("mart_daily_store_sales",daily)

    online_daily=osales.groupby(["order_date","fulfillment_store_id","fulfillment_type"],as_index=False).agg(order_count=("online_order_id","nunique"),units=("quantity","sum"),net_sales=("net_sales","sum"),gross_margin=("gross_margin","sum")); gold("mart_daily_online_sales",online_daily)

    omni_store=daily.groupby("store_id",as_index=False).agg(store_sales=("net_sales","sum"),store_margin=("gross_margin","sum"))
    omni_online=online_daily.groupby("fulfillment_store_id",as_index=False).agg(online_sales=("net_sales","sum"),online_margin=("gross_margin","sum")).rename(columns={"fulfillment_store_id":"store_id"})
    omni=stores[["store_id","store_name","region","store_format"]].merge(omni_store,on="store_id",how="left").merge(omni_online,on="store_id",how="left").fillna(0); omni["total_sales"]=omni["store_sales"]+omni["online_sales"]; omni["digital_mix"]=safe_div(omni["online_sales"],omni["total_sales"]); gold("mart_omnichannel_store_performance",omni.sort_values("total_sales",ascending=False))

    prod_perf=sales.groupby(["product_id","department","subcategory","brand"],as_index=False).agg(units_sold=("quantity","sum"),net_sales=("net_sales","sum"),gross_margin=("gross_margin","sum"),transaction_count=("transaction_id","nunique"))
    rprod=returns[returns["product_id"].isin(products["product_id"])].groupby("product_id",as_index=False).agg(return_qty=("return_qty","sum"),refund_amount=("refund_amount","sum"),return_count=("return_id","count"))
    prod_perf=prod_perf.merge(rprod,on="product_id",how="left").fillna({"return_qty":0,"refund_amount":0,"return_count":0}); prod_perf["return_rate"]=safe_div(prod_perf["return_qty"],prod_perf["units_sold"]); gold("mart_product_performance",prod_perf.sort_values("net_sales",ascending=False))

    cust_store=sales[sales["customer_id"]!=""].groupby("customer_id",as_index=False).agg(store_transactions=("transaction_id","nunique"),store_sales=("net_sales","sum"),store_units=("quantity","sum"))
    cust_web=osales.groupby("customer_id",as_index=False).agg(online_orders=("online_order_id","nunique"),online_sales=("net_sales","sum"),online_units=("quantity","sum"))
    cust=customers.merge(cust_store,on="customer_id",how="left").merge(cust_web,on="customer_id",how="left").fillna({"store_transactions":0,"store_sales":0,"store_units":0,"online_orders":0,"online_sales":0,"online_units":0}); cust["lifetime_sales"]=cust["store_sales"]+cust["online_sales"]; cust["omnichannel_flag"]=(cust["store_transactions"]>0)&(cust["online_orders"]>0); gold("mart_customer_360",cust.sort_values("lifetime_sales",ascending=False))

    promo=sales[sales["promotion_id"]!=""].groupby("promotion_id",as_index=False).agg(transaction_count=("transaction_id","nunique"),units=("quantity","sum"),promo_sales=("net_sales","sum"),markdown_amount=("markdown_amount","sum"))
    promo=promo.merge(data["promotions"],on="promotion_id",how="left"); gold("mart_promotion_performance",promo.sort_values("promo_sales",ascending=False))

    store_inv=latest.groupby("store_id",as_index=False).agg(sku_count=("product_id","nunique"),on_hand_qty=("on_hand_qty","sum"),available_qty=("available_qty","sum"),stockout_sku_count=("stockout_flag","sum"),low_stock_sku_count=("low_stock_flag","sum"),overstock_sku_count=("overstock_flag","sum"))
    store_inv=store_inv.merge(stores[["store_id","store_name","region","square_feet"]],on="store_id",how="left"); store_inv["inventory_density"]=safe_div(store_inv["on_hand_qty"],store_inv["square_feet"]); gold("mart_store_inventory_health",store_inv.sort_values("stockout_sku_count",ascending=False))

    ship_perf=ship[ship["online_order_id"].isin(online["online_order_id"])].groupby("carrier",as_index=False).agg(shipment_count=("shipment_id","count"),late_shipments=("late_delivery_flag","sum"),avg_delivery_days=("delivery_days","mean"),shipping_cost=("shipping_cost","sum")); ship_perf["late_rate"]=safe_div(ship_perf["late_shipments"],ship_perf["shipment_count"]); gold("mart_carrier_performance",ship_perf)

    ret_by_cust=returns.merge(pos[["transaction_id","customer_id"]],left_on="original_transaction_id",right_on="transaction_id",how="left").groupby("customer_id",as_index=False).agg(return_count=("return_id","count"),refund_amount=("refund_amount","sum"))
    crisk=cust[["customer_id","store_transactions","online_orders","lifetime_sales"]].merge(ret_by_cust,on="customer_id",how="left").fillna({"return_count":0,"refund_amount":0}); crisk["purchase_count"]=crisk["store_transactions"]+crisk["online_orders"]; crisk["return_rate"]=safe_div(crisk["return_count"],crisk["purchase_count"]); crisk["suspicious_return_flag"]=crisk["return_rate"]>=CFG["suspicious_return_rate_threshold"]; gold("mart_return_risk",crisk.sort_values("return_rate",ascending=False))

    for name,df in {"exception_payment_mismatches":recon[recon["reconciliation_status"]=="Mismatch"],"exception_stockouts":latest[latest["stockout_flag"]],
                    "exception_high_value_baskets":baskets[baskets["high_value_basket_flag"]],"exception_suspicious_returns":crisk[crisk["suspicious_return_flag"]],
                    "exception_late_shipments":ship[ship["late_delivery_flag"]]}.items(): gold(name,df)

    dq=[]
    for n,c in raw_counts.items(): dq.append({"check_name":f"{n}_raw_to_silver_delta","table_name":n,"issue_count":c-len(data[n]),"severity":"info"})
    for n,c in qs.items(): dq.append({"check_name":n,"table_name":n.split("_")[0],"issue_count":c,"severity":"warning" if c else "pass"})
    write_csv(pd.DataFrame(dq),REPORT/"data_quality_report.csv")
    write_csv(pd.DataFrame([{"pipeline_name":CFG["project_name"],"run_timestamp_utc":pd.Timestamp.utcnow().isoformat(),"raw_file_count":len(RAW),"raw_total_rows":sum(raw_counts.values()),"silver_total_rows":sum(len(v) for v in data.values()),"quarantine_total_rows":sum(qs.values()),"gold_table_count":len(list(GOLD.glob("*.csv"))),"status":"SUCCESS"}]),REPORT/"pipeline_audit_log.csv")
    meta={"last_successful_run_utc":pd.Timestamp.utcnow().isoformat(),"pos_transaction_watermark":str(pos["transaction_ts"].max()),"online_order_watermark":str(online["order_ts"].max()),"inventory_adjustment_watermark":str(adj["adjustment_ts"].max()),"status":"SUCCESS"}
    (META/"run_watermarks.json").write_text(json.dumps(meta,indent=2),encoding="utf-8")
    print("Walmart-style enterprise retail ETL complete.")
    print(f"Gold tables: {len(list(GOLD.glob('*.csv')))}; quarantine tables: {len(list(Q.glob('*.csv')))}")

if __name__=="__main__": main()
