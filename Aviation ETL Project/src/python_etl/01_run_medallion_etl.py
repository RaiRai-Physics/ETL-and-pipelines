from pathlib import Path
import sys,json
import pandas as pd
import numpy as np

ROOT=Path(__file__).resolve().parents[2]; sys.path.append(str(ROOT))
from src.common.utils import load_config,read_csv,write_csv,clean_text,parse_date,parse_ts,parse_num,parse_bool,stable_hash,safe_div

CFG=load_config()
BRONZE=ROOT/CFG["bronze_path"]; SILVER=ROOT/CFG["silver_path"]; Q=ROOT/CFG["quarantine_path"]; GOLD=ROOT/CFG["gold_path"]; REPORT=ROOT/CFG["report_path"]; META=ROOT/CFG["metadata_path"]

RAW={
"airports":"data/raw/master/airports.csv","aircraft":"data/raw/master/aircraft.csv","fare_classes":"data/raw/master/fare_classes.csv",
"loyalty_members":"data/raw/master/loyalty_members.csv","passengers":"data/raw/master/passengers.csv","crew":"data/raw/master/crew.csv",
"calendar":"data/raw/reference/calendar.csv","routes":"data/raw/reference/routes.csv","weather_daily":"data/raw/reference/weather_daily.csv",
"flight_schedules":"data/raw/operations/flight_schedules.csv","flight_events":"data/raw/operations/flight_events.csv","crew_assignments":"data/raw/operations/crew_assignments.csv",
"bookings":"data/raw/booking/bookings.csv","booking_segments":"data/raw/booking/booking_segments.csv","tickets":"data/raw/booking/tickets.csv","baggage":"data/raw/booking/baggage.csv",
"payments":"data/raw/revenue/payments.csv","refunds":"data/raw/revenue/refunds.csv","maintenance_events":"data/raw/maintenance/maintenance_events.csv",
"loyalty_cdc":"data/raw/cdc/loyalty_cdc.csv","aircraft_cdc":"data/raw/cdc/aircraft_cdc.csv"}

def raw(name): return read_csv(ROOT/RAW[name])
def bronze(name,df):
    x=df.copy(); x["_bronze_loaded_at_utc"]=pd.Timestamp.utcnow().isoformat(); x["_source_file"]=RAW[name]
    write_csv(x,BRONZE/f"{name}.csv")
def silver(name,df): write_csv(df,SILVER/f"{name}.csv")
def gold(name,df): write_csv(df,GOLD/f"{name}.csv")
def quarantine(name,df,reason):
    x=df.copy(); x["quarantine_reason"]=reason
    if len(x)==0: x=pd.DataFrame(columns=["quarantine_reason"])
    write_csv(x,Q/f"{name}.csv")

def c_airports(df):
    x=df.copy()
    for c in ["airport_code","country","timezone_group"]: x[c]=clean_text(x[c],"","upper")
    for c in ["airport_name","city"]: x[c]=clean_text(x[c],case="title")
    x["state"]=clean_text(x["state"],"", "upper"); x["active_flag"]=parse_bool(x["active_flag"])
    return x.drop_duplicates("airport_code")

def c_aircraft(df):
    x=df.copy(); x["aircraft_id"]=clean_text(x["aircraft_id"],"", "upper"); x["tail_number"]=clean_text(x["tail_number"],"", "upper")
    x["model_code"]=clean_text(x["model_code"],"", "upper"); x["model_name"]=clean_text(x["model_name"],case="title")
    x["seat_capacity"]=parse_num(x["seat_capacity"],0); x["range_nm"]=parse_num(x["range_nm"],0); x["manufacture_year"]=parse_num(x["manufacture_year"])
    x["in_service_date"]=parse_date(x["in_service_date"]); x["aircraft_status"]=clean_text(x["aircraft_status"],case="title")
    return x.drop_duplicates("aircraft_id")

def c_fares(df):
    x=df.copy(); x["fare_class"]=clean_text(x["fare_class"],"", "upper"); x["cabin"]=clean_text(x["cabin"],case="title")
    x["refundable_flag"]=parse_bool(x["refundable_flag"]); x["changeable_flag"]=parse_bool(x["changeable_flag"])
    return x.drop_duplicates("fare_class")

def c_loyalty(df):
    x=df.copy(); x["loyalty_id"]=clean_text(x["loyalty_id"],"", "upper"); x["member_name"]=clean_text(x["member_name"],case="title")
    x["join_date"]=parse_date(x["join_date"]); x["loyalty_tier"]=clean_text(x["loyalty_tier"],"Unknown","title"); x["home_airport"]=clean_text(x["home_airport"],"", "upper")
    x["lifetime_miles"]=parse_num(x["lifetime_miles"],0); x["active_flag"]=parse_bool(x["active_flag"])
    return x.drop_duplicates("loyalty_id")

def c_passengers(df):
    x=df.copy(); x["passenger_id"]=clean_text(x["passenger_id"],"", "upper"); x["passenger_name"]=clean_text(x["passenger_name"],case="title")
    x["date_of_birth"]=parse_date(x["date_of_birth"]); x["gender"]=clean_text(x["gender"],"Unknown","title").replace({"F":"Female","M":"Male"})
    x["email"]=clean_text(x["email"],"unknown@example.com","lower"); x["country"]=clean_text(x["country"],"", "upper"); x["loyalty_id"]=clean_text(x["loyalty_id"],"", "upper")
    x["active_flag"]=parse_bool(x["active_flag"]); x["passenger_hash"]=x["passenger_id"].map(stable_hash)
    return x.drop_duplicates("passenger_id")

def c_crew(df):
    x=df.copy(); x["crew_id"]=clean_text(x["crew_id"],"", "upper"); x["crew_name"]=clean_text(x["crew_name"],case="title"); x["role"]=clean_text(x["role"],case="title")
    x["base_airport"]=clean_text(x["base_airport"],"", "upper"); x["hire_date"]=parse_date(x["hire_date"]); x["employment_status"]=clean_text(x["employment_status"],case="title")
    return x.drop_duplicates("crew_id")

def c_calendar(df):
    x=df.copy(); x["calendar_date"]=parse_date(x["calendar_date"]); return x.drop_duplicates("date_key")

def c_routes(df):
    x=df.copy(); x["route_id"]=clean_text(x["route_id"],"", "upper"); x["origin_airport"]=clean_text(x["origin_airport"],"", "upper"); x["destination_airport"]=clean_text(x["destination_airport"],"", "upper")
    x["distance_miles"]=parse_num(x["distance_miles"],0); x["scheduled_block_minutes"]=parse_num(x["scheduled_block_minutes"],0)
    return x.drop_duplicates("route_id")

def c_weather(df):
    x=df.copy(); x["airport_code"]=clean_text(x["airport_code"],"", "upper"); x["weather_date"]=parse_date(x["weather_date"])
    for c in ["temperature_c","wind_speed_kts","visibility_miles"]: x[c]=parse_num(x[c])
    x["weather_condition"]=clean_text(x["weather_condition"],case="title")
    return x.drop_duplicates(["airport_code","weather_date"],keep="last")

def c_schedule(df):
    x=df.copy(); x["flight_id"]=clean_text(x["flight_id"],"", "upper"); x["flight_number"]=clean_text(x["flight_number"],"", "upper"); x["route_id"]=clean_text(x["route_id"],"", "upper"); x["aircraft_id"]=clean_text(x["aircraft_id"],"", "upper")
    x["scheduled_departure_ts"]=parse_ts(x["scheduled_departure_ts"]); x["scheduled_arrival_ts"]=parse_ts(x["scheduled_arrival_ts"]); x["flight_date"]=x["scheduled_departure_ts"].dt.date.astype("string")
    x["flight_status"]=clean_text(x["flight_status"],case="title"); return x.drop_duplicates("flight_id")

def c_events(df):
    x=df.copy(); x["flight_event_id"]=clean_text(x["flight_event_id"],"", "upper"); x["flight_id"]=clean_text(x["flight_id"],"", "upper")
    x["actual_departure_ts"]=parse_ts(x["actual_departure_ts"]); x["actual_arrival_ts"]=parse_ts(x["actual_arrival_ts"])
    x["departure_delay_minutes"]=parse_num(x["departure_delay_minutes"]); x["arrival_delay_minutes"]=parse_num(x["arrival_delay_minutes"],0)
    x["delay_code"]=clean_text(x["delay_code"],"NONE","upper"); x["operational_status"]=clean_text(x["operational_status"],case="title")
    return x.drop_duplicates("flight_event_id")

def c_assign(df):
    x=df.copy(); x["crew_assignment_id"]=clean_text(x["crew_assignment_id"],"", "upper"); x["flight_id"]=clean_text(x["flight_id"],"", "upper"); x["crew_id"]=clean_text(x["crew_id"],"", "upper")
    x["role"]=clean_text(x["role"],case="title"); x["assignment_status"]=clean_text(x["assignment_status"],case="title"); return x.drop_duplicates("crew_assignment_id")

def c_bookings(df):
    x=df.copy(); x["booking_id"]=clean_text(x["booking_id"],"", "upper"); x["passenger_id"]=clean_text(x["passenger_id"],"", "upper"); x["booking_ts"]=parse_ts(x["booking_ts"])
    x["booking_channel"]=clean_text(x["booking_channel"],case="title"); x["booking_status"]=clean_text(x["booking_status"],case="title"); x["currency"]=clean_text(x["currency"],"USD","upper"); x["total_booking_amount"]=parse_num(x["total_booking_amount"])
    return x.drop_duplicates("booking_id")

def c_segments(df):
    x=df.copy(); x["booking_segment_id"]=clean_text(x["booking_segment_id"],"", "upper"); x["booking_id"]=clean_text(x["booking_id"],"", "upper"); x["flight_id"]=clean_text(x["flight_id"],"", "upper")
    x["fare_class"]=clean_text(x["fare_class"],"", "upper"); x["seat_number"]=clean_text(x["seat_number"],"Unknown","upper"); x["segment_status"]=clean_text(x["segment_status"],case="title")
    for c in ["base_fare","tax_amount","ancillary_amount"]: x[c]=parse_num(x[c],0)
    return x.drop_duplicates("booking_segment_id")

def c_tickets(df):
    x=df.copy(); x["ticket_id"]=clean_text(x["ticket_id"],"", "upper"); x["booking_id"]=clean_text(x["booking_id"],"", "upper"); x["passenger_id"]=clean_text(x["passenger_id"],"", "upper")
    x["issue_date"]=parse_date(x["issue_date"]); x["ticket_status"]=clean_text(x["ticket_status"],case="title"); x["ticket_amount"]=parse_num(x["ticket_amount"])
    return x.drop_duplicates("ticket_id")

def c_baggage(df):
    x=df.copy(); x["bag_id"]=clean_text(x["bag_id"],"", "upper"); x["booking_id"]=clean_text(x["booking_id"],"", "upper"); x["flight_id"]=clean_text(x["flight_id"],"", "upper"); x["passenger_id"]=clean_text(x["passenger_id"],"", "upper")
    x["bag_weight_kg"]=parse_num(x["bag_weight_kg"]); x["bag_fee"]=parse_num(x["bag_fee"],0); x["bag_status"]=clean_text(x["bag_status"],case="title")
    return x.drop_duplicates("bag_id")

def c_payments(df):
    x=df.copy(); x["payment_id"]=clean_text(x["payment_id"],"", "upper"); x["booking_id"]=clean_text(x["booking_id"],"", "upper"); x["payment_ts"]=parse_ts(x["payment_ts"])
    x["payment_method"]=clean_text(x["payment_method"],case="title"); x["payment_status"]=clean_text(x["payment_status"],case="title"); x["payment_amount"]=parse_num(x["payment_amount"])
    return x.drop_duplicates("payment_id")

def c_refunds(df):
    x=df.copy(); x["refund_id"]=clean_text(x["refund_id"],"", "upper"); x["ticket_id"]=clean_text(x["ticket_id"],"", "upper"); x["refund_date"]=parse_date(x["refund_date"])
    x["refund_amount"]=parse_num(x["refund_amount"],0); x["refund_reason"]=clean_text(x["refund_reason"],case="title"); x["refund_status"]=clean_text(x["refund_status"],case="title")
    return x.drop_duplicates("refund_id")

def c_mx(df):
    x=df.copy(); x["maintenance_event_id"]=clean_text(x["maintenance_event_id"],"", "upper"); x["aircraft_id"]=clean_text(x["aircraft_id"],"", "upper")
    x["event_start_ts"]=parse_ts(x["event_start_ts"]); x["event_end_ts"]=parse_ts(x["event_end_ts"]); x["maintenance_type"]=clean_text(x["maintenance_type"],case="title")
    x["ata_chapter"]=clean_text(x["ata_chapter"],"Unknown","upper"); x["labor_hours"]=parse_num(x["labor_hours"]); x["parts_cost"]=parse_num(x["parts_cost"],0); x["event_status"]=clean_text(x["event_status"],case="title")
    x["duration_hours"]=(x["event_end_ts"]-x["event_start_ts"]).dt.total_seconds()/3600
    return x.drop_duplicates("maintenance_event_id")

def c_loyalty_cdc(df):
    x=df.copy(); x["cdc_id"]=clean_text(x["cdc_id"],"", "upper"); x["loyalty_id"]=clean_text(x["loyalty_id"],"", "upper"); x["effective_ts"]=parse_ts(x["effective_ts"]); x["effective_date"]=x["effective_ts"].dt.date.astype("string")
    x["operation"]=clean_text(x["operation"],case="upper"); x["new_loyalty_tier"]=clean_text(x["new_loyalty_tier"],case="title"); x["new_active_flag"]=parse_bool(x["new_active_flag"])
    return x.drop_duplicates("cdc_id")

def c_aircraft_cdc(df):
    x=df.copy(); x["cdc_id"]=clean_text(x["cdc_id"],"", "upper"); x["aircraft_id"]=clean_text(x["aircraft_id"],"", "upper"); x["effective_ts"]=parse_ts(x["effective_ts"]); x["effective_date"]=x["effective_ts"].dt.date.astype("string")
    x["operation"]=clean_text(x["operation"],case="upper"); x["new_aircraft_status"]=clean_text(x["new_aircraft_status"],case="title"); return x.drop_duplicates("cdc_id")

CLEAN={"airports":c_airports,"aircraft":c_aircraft,"fare_classes":c_fares,"loyalty_members":c_loyalty,"passengers":c_passengers,"crew":c_crew,
"calendar":c_calendar,"routes":c_routes,"weather_daily":c_weather,"flight_schedules":c_schedule,"flight_events":c_events,"crew_assignments":c_assign,
"bookings":c_bookings,"booking_segments":c_segments,"tickets":c_tickets,"baggage":c_baggage,"payments":c_payments,"refunds":c_refunds,
"maintenance_events":c_mx,"loyalty_cdc":c_loyalty_cdc,"aircraft_cdc":c_aircraft_cdc}

def loyalty_scd2(base,cdc):
    b=base[["loyalty_id","member_name","join_date","loyalty_tier","home_airport","lifetime_miles","active_flag"]].copy(); b["effective_start_date"]=b["join_date"].fillna("1900-01-01"); b["source"]="member_master"
    u=cdc.merge(base[["loyalty_id","member_name","join_date","home_airport","lifetime_miles"]],on="loyalty_id",how="inner")
    u=u.rename(columns={"new_loyalty_tier":"loyalty_tier","new_active_flag":"active_flag","effective_date":"effective_start_date"}); u["source"]="loyalty_cdc"; u=u[b.columns]
    x=pd.concat([b,u],ignore_index=True); x["_d"]=pd.to_datetime(x["effective_start_date"],errors="coerce"); x=x.sort_values(["loyalty_id","_d"])
    x["effective_end_date"]=x.groupby("loyalty_id")["_d"].shift(-1).dt.date.astype("string").fillna("9999-12-31"); x["is_current"]=x["effective_end_date"].eq("9999-12-31")
    x["loyalty_sk"]=[stable_hash(a,b,c) for a,b,c in zip(x["loyalty_id"],x["effective_start_date"],x["loyalty_tier"])]
    return x.drop(columns="_d")

def aircraft_scd2(base,cdc):
    b=base.copy(); b["effective_start_date"]=b["in_service_date"].fillna("1900-01-01"); b["source"]="aircraft_master"
    u=cdc.merge(base.drop(columns=["aircraft_status"]),on="aircraft_id",how="inner"); u["aircraft_status"]=u["new_aircraft_status"]; u["effective_start_date"]=u["effective_date"]; u["source"]="aircraft_cdc"; u=u[b.columns]
    x=pd.concat([b,u],ignore_index=True); x["_d"]=pd.to_datetime(x["effective_start_date"],errors="coerce"); x=x.sort_values(["aircraft_id","_d"])
    x["effective_end_date"]=x.groupby("aircraft_id")["_d"].shift(-1).dt.date.astype("string").fillna("9999-12-31"); x["is_current"]=x["effective_end_date"].eq("9999-12-31")
    x["aircraft_sk"]=[stable_hash(a,b,c) for a,b,c in zip(x["aircraft_id"],x["effective_start_date"],x["aircraft_status"])]
    return x.drop(columns="_d")

def main():
    for d in [BRONZE,SILVER,Q,GOLD,REPORT,META]: d.mkdir(parents=True,exist_ok=True)
    data={}; raw_counts={}
    for name in RAW:
        r=raw(name); raw_counts[name]=len(r); bronze(name,r); data[name]=CLEAN[name](r); silver(name,data[name])

    airports,aircraft,loyalty,passengers,crew=data["airports"],data["aircraft"],data["loyalty_members"],data["passengers"],data["crew"]
    routes,schedules,events,assign=data["routes"],data["flight_schedules"],data["flight_events"],data["crew_assignments"]
    bookings,segments,tickets,bags=data["bookings"],data["booking_segments"],data["tickets"],data["baggage"]
    payments,refunds,mx=data["payments"],data["refunds"],data["maintenance_events"]

    qs={}
    def q(name,df,reason): quarantine(name,df,reason); qs[name]=len(df)
    q("passengers_invalid_loyalty",passengers[(passengers["loyalty_id"]!="")&(~passengers["loyalty_id"].isin(loyalty["loyalty_id"]))],"Missing loyalty member")
    q("schedules_invalid_aircraft",schedules[~schedules["aircraft_id"].isin(aircraft["aircraft_id"])],"Missing aircraft")
    q("schedules_invalid_route",schedules[~schedules["route_id"].isin(routes["route_id"])],"Missing route")
    q("events_invalid_flight",events[~events["flight_id"].isin(schedules["flight_id"])],"Missing flight schedule")
    q("events_bad_delay",events[events["departure_delay_minutes"].isna()],"Invalid delay minutes")
    q("crew_assignment_invalid_flight",assign[~assign["flight_id"].isin(schedules["flight_id"])],"Missing flight")
    q("crew_assignment_invalid_crew",assign[~assign["crew_id"].isin(crew["crew_id"])],"Missing crew")
    q("bookings_invalid_passenger",bookings[~bookings["passenger_id"].isin(passengers["passenger_id"])],"Missing passenger")
    q("bookings_bad_amount",bookings[bookings["total_booking_amount"].isna()],"Invalid booking amount")
    q("segments_invalid_booking",segments[~segments["booking_id"].isin(bookings["booking_id"])],"Missing booking")
    q("segments_invalid_flight",segments[~segments["flight_id"].isin(schedules["flight_id"])],"Missing flight")
    q("payments_invalid_booking",payments[~payments["booking_id"].isin(bookings["booking_id"])],"Missing booking")
    q("payments_bad_amount",payments[payments["payment_amount"].isna()],"Invalid payment amount")
    q("refunds_invalid_ticket",refunds[~refunds["ticket_id"].isin(tickets["ticket_id"])],"Missing ticket")
    q("maintenance_invalid_aircraft",mx[~mx["aircraft_id"].isin(aircraft["aircraft_id"])],"Missing aircraft")
    q("maintenance_bad_labor",mx[mx["labor_hours"].isna()],"Invalid labor hours")

    lscd=loyalty_scd2(loyalty,data["loyalty_cdc"]); ascd=aircraft_scd2(aircraft,data["aircraft_cdc"])
    gold("dim_loyalty_scd2",lscd); gold("dim_loyalty_current",lscd[lscd["is_current"]]); gold("dim_aircraft_scd2",ascd); gold("dim_aircraft_current",ascd[ascd["is_current"]])
    for n,df in {"dim_airport":airports,"dim_passenger":passengers,"dim_crew":crew,"dim_route":routes,"dim_fare_class":data["fare_classes"]}.items(): gold(n,df)

    valid_sched=schedules[schedules["aircraft_id"].isin(aircraft["aircraft_id"])&schedules["route_id"].isin(routes["route_id"])].copy()
    fact_flights=valid_sched.merge(events[events["flight_id"].isin(valid_sched["flight_id"])],on="flight_id",how="left").merge(routes,on="route_id",how="left").merge(aircraft[["aircraft_id","seat_capacity","model_code"]],on="aircraft_id",how="left")
    fact_flights["departure_delay_minutes"]=fact_flights["departure_delay_minutes"].fillna(0); fact_flights["arrival_delay_minutes"]=fact_flights["arrival_delay_minutes"].fillna(0)
    fact_flights["on_time_departure_flag"]=fact_flights["departure_delay_minutes"]<=CFG["on_time_threshold_minutes"]; fact_flights["long_delay_flag"]=fact_flights["departure_delay_minutes"]>=CFG["long_delay_threshold_minutes"]
    fact_flights["cancelled_flag"]=fact_flights["flight_status"].eq("Cancelled"); gold("fact_flights",fact_flights)

    valid_segments=segments[segments["booking_id"].isin(bookings["booking_id"])&segments["flight_id"].isin(valid_sched["flight_id"])].copy()
    booking_fact=valid_segments.merge(bookings[["booking_id","passenger_id","booking_ts","booking_channel","booking_status","total_booking_amount"]],on="booking_id",how="left").merge(valid_sched[["flight_id","route_id","aircraft_id","flight_date"]],on="flight_id",how="left")
    booking_fact["segment_revenue"]=booking_fact["base_fare"]+booking_fact["tax_amount"]+booking_fact["ancillary_amount"]; booking_fact["flown_flag"]=booking_fact["segment_status"].eq("Flown"); booking_fact["no_show_flag"]=booking_fact["segment_status"].eq("No Show"); gold("fact_booking_segments",booking_fact)

    valid_bags=bags[bags["booking_id"].isin(bookings["booking_id"])&bags["flight_id"].isin(valid_sched["flight_id"])].copy(); valid_bags["mishandled_flag"]=valid_bags["bag_status"].eq("Mishandled"); gold("fact_baggage",valid_bags)
    valid_mx=mx[mx["aircraft_id"].isin(aircraft["aircraft_id"])].copy(); valid_mx["aog_flag"]=valid_mx["maintenance_type"].eq("Aog")|((valid_mx["duration_hours"]>=CFG["maintenance_aog_hours_alert"])&valid_mx["event_status"].ne("Closed")); gold("fact_maintenance",valid_mx)

    captured=payments[payments["payment_status"].eq("Captured")].groupby("booking_id",as_index=False).agg(captured_amount=("payment_amount","sum"),payment_count=("payment_id","count"))
    brecon=bookings[["booking_id","total_booking_amount","booking_status"]].merge(captured,on="booking_id",how="left").fillna({"captured_amount":0,"payment_count":0}); brecon["difference"]=brecon["captured_amount"]-brecon["total_booking_amount"].fillna(0); brecon["reconciliation_status"]=np.where(brecon["difference"].abs()<=CFG["payment_match_tolerance"],"Matched","Mismatch"); gold("recon_bookings_vs_payments",brecon)

    refund_sum=refunds[refunds["refund_status"].eq("Processed")].merge(tickets[["ticket_id","booking_id"]],on="ticket_id",how="left").groupby("booking_id",as_index=False).agg(refund_amount=("refund_amount","sum"),refund_count=("refund_id","count"))
    revenue=booking_fact.groupby("booking_id",as_index=False).agg(segment_revenue=("segment_revenue","sum"),segment_count=("booking_segment_id","count")).merge(refund_sum,on="booking_id",how="left").fillna({"refund_amount":0,"refund_count":0}); revenue["net_revenue"]=revenue["segment_revenue"]-revenue["refund_amount"]; gold("fact_booking_revenue",revenue)

    flight_load=booking_fact.groupby("flight_id",as_index=False).agg(booked_seats=("booking_segment_id","count"),flown_passengers=("flown_flag","sum"),no_shows=("no_show_flag","sum"),passenger_revenue=("segment_revenue","sum"))
    flight_perf=fact_flights.merge(flight_load,on="flight_id",how="left").fillna({"booked_seats":0,"flown_passengers":0,"no_shows":0,"passenger_revenue":0})
    flight_perf["load_factor"]=safe_div(flight_perf["flown_passengers"],flight_perf["seat_capacity"]); flight_perf["revenue_per_available_seat"]=safe_div(flight_perf["passenger_revenue"],flight_perf["seat_capacity"]); gold("mart_flight_performance",flight_perf)

    route_perf=flight_perf.groupby(["route_id","origin_airport","destination_airport"],as_index=False).agg(flight_count=("flight_id","count"),cancelled_flights=("cancelled_flag","sum"),avg_departure_delay=("departure_delay_minutes","mean"),on_time_flights=("on_time_departure_flag","sum"),passengers=("flown_passengers","sum"),revenue=("passenger_revenue","sum"),avg_load_factor=("load_factor","mean"))
    route_perf["on_time_rate"]=safe_div(route_perf["on_time_flights"],route_perf["flight_count"]); route_perf["cancellation_rate"]=safe_div(route_perf["cancelled_flights"],route_perf["flight_count"]); gold("mart_route_performance",route_perf.sort_values("revenue",ascending=False))

    bag_perf=valid_bags.groupby("flight_id",as_index=False).agg(checked_bags=("bag_id","count"),mishandled_bags=("mishandled_flag","sum"),bag_fee_revenue=("bag_fee","sum")); bag_perf["mishandling_rate"]=safe_div(bag_perf["mishandled_bags"],bag_perf["checked_bags"]); gold("mart_baggage_performance",bag_perf)

    ac_ops=fact_flights.groupby("aircraft_id",as_index=False).agg(flight_count=("flight_id","count"),flight_miles=("distance_miles","sum"),long_delay_count=("long_delay_flag","sum"))
    ac_mx=valid_mx.groupby("aircraft_id",as_index=False).agg(maintenance_events=("maintenance_event_id","count"),maintenance_hours=("duration_hours","sum"),maintenance_cost=("parts_cost","sum"),aog_events=("aog_flag","sum"))
    ac_rel=aircraft.merge(ac_ops,on="aircraft_id",how="left").merge(ac_mx,on="aircraft_id",how="left").fillna({"flight_count":0,"flight_miles":0,"long_delay_count":0,"maintenance_events":0,"maintenance_hours":0,"maintenance_cost":0,"aog_events":0}); ac_rel["mx_events_per_100_flights"]=safe_div(ac_rel["maintenance_events"]*100,ac_rel["flight_count"]); gold("mart_aircraft_reliability",ac_rel.sort_values("aog_events",ascending=False))

    crew_load=assign[assign["flight_id"].isin(valid_sched["flight_id"])&assign["crew_id"].isin(crew["crew_id"])].groupby(["crew_id","role"],as_index=False).agg(assigned_flights=("flight_id","nunique"))
    crew_load=crew_load.merge(crew[["crew_id","crew_name","base_airport","employment_status"]],on="crew_id",how="left"); gold("mart_crew_workload",crew_load.sort_values("assigned_flights",ascending=False))

    pax_rev=booking_fact.groupby("passenger_id",as_index=False).agg(bookings=("booking_id","nunique"),flights_booked=("flight_id","nunique"),segment_revenue=("segment_revenue","sum"),flown_segments=("flown_flag","sum"),no_shows=("no_show_flag","sum"))
    pax=passengers.merge(pax_rev,on="passenger_id",how="left").fillna({"bookings":0,"flights_booked":0,"segment_revenue":0,"flown_segments":0,"no_shows":0}).merge(loyalty[["loyalty_id","loyalty_tier","lifetime_miles"]],on="loyalty_id",how="left")
    pax["no_show_rate"]=safe_div(pax["no_shows"],pax["flights_booked"]); pax["value_tier"]=pd.cut(pax["segment_revenue"],[-1,500,2000,5000,float("inf")],labels=["Bronze","Silver","Gold","Platinum"]).astype("string"); gold("mart_passenger_360",pax.sort_values("segment_revenue",ascending=False))

    loy_perf=pax[pax["loyalty_id"]!=""].groupby("loyalty_tier",dropna=False,as_index=False).agg(member_count=("loyalty_id","nunique"),passenger_count=("passenger_id","count"),flights=("flown_segments","sum"),revenue=("segment_revenue","sum")); loy_perf["revenue_per_member"]=safe_div(loy_perf["revenue"],loy_perf["member_count"]); gold("mart_loyalty_performance",loy_perf)

    airport_dep=flight_perf.groupby("origin_airport",as_index=False).agg(departures=("flight_id","count"),avg_delay=("departure_delay_minutes","mean"),long_delays=("long_delay_flag","sum"),cancelled=("cancelled_flag","sum"),passengers=("flown_passengers","sum"))
    airport_dep=airport_dep.merge(airports[["airport_code","airport_name","city","state"]],left_on="origin_airport",right_on="airport_code",how="left"); airport_dep["long_delay_rate"]=safe_div(airport_dep["long_delays"],airport_dep["departures"]); gold("mart_airport_operations",airport_dep.sort_values("avg_delay",ascending=False))

    refund_risk=revenue.copy(); refund_risk["refund_rate"]=safe_div(refund_risk["refund_amount"],refund_risk["segment_revenue"]); refund_risk["high_refund_flag"]=refund_risk["refund_rate"]>=CFG["high_refund_rate"]; gold("mart_refund_risk",refund_risk.sort_values("refund_rate",ascending=False))

    for n,df in {
        "exception_long_delays":flight_perf[flight_perf["long_delay_flag"]],
        "exception_cancelled_flights":flight_perf[flight_perf["cancelled_flag"]],
        "exception_baggage_mishandling":bag_perf[bag_perf["mishandling_rate"]>=CFG["high_baggage_mishandle_rate"]],
        "exception_payment_mismatches":brecon[brecon["reconciliation_status"]=="Mismatch"],
        "exception_high_refunds":refund_risk[refund_risk["high_refund_flag"]],
        "exception_aircraft_aog":valid_mx[valid_mx["aog_flag"]]
    }.items(): gold(n,df)

    dq=[]
    for n,c in raw_counts.items(): dq.append({"check_name":f"{n}_raw_to_silver_delta","table_name":n,"issue_count":c-len(data[n]),"severity":"info"})
    for n,c in qs.items(): dq.append({"check_name":n,"table_name":n.split("_")[0],"issue_count":c,"severity":"warning" if c else "pass"})
    write_csv(pd.DataFrame(dq),REPORT/"data_quality_report.csv")
    write_csv(pd.DataFrame([{"pipeline_name":CFG["project_name"],"run_timestamp_utc":pd.Timestamp.utcnow().isoformat(),"raw_file_count":len(RAW),"raw_total_rows":sum(raw_counts.values()),"silver_total_rows":sum(len(v) for v in data.values()),"quarantine_total_rows":sum(qs.values()),"gold_table_count":len(list(GOLD.glob("*.csv"))),"status":"SUCCESS"}]),REPORT/"pipeline_audit_log.csv")
    meta={"last_successful_run_utc":pd.Timestamp.utcnow().isoformat(),"flight_watermark":str(schedules["scheduled_departure_ts"].max()),"booking_watermark":str(bookings["booking_ts"].max()),"maintenance_watermark":str(mx["event_start_ts"].max()),"status":"SUCCESS"}
    (META/"run_watermarks.json").write_text(json.dumps(meta,indent=2),encoding="utf-8")
    print(f"Aviation Medallion ETL complete. Gold={len(list(GOLD.glob('*.csv')))} Quarantine={len(list(Q.glob('*.csv')))}")

if __name__=="__main__": main()
