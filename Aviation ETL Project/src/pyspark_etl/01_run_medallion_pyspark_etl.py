from pathlib import Path
from pyspark.sql import SparkSession,functions as F

ROOT=Path(__file__).resolve().parents[2]
RAW=ROOT/"data/raw"; SILVER=ROOT/"data/silver/pyspark"; GOLD=ROOT/"data/gold/pyspark"; Q=ROOT/"data/quarantine/pyspark"; REPORT=ROOT/"reports/pyspark"

def spark(): return SparkSession.builder.appName("AviationMedallionETL").master("local[*]").getOrCreate()
def read(s,rel): return s.read.option("header",True).option("inferSchema",False).csv(str(RAW/rel))
def write(df,path): df.coalesce(1).write.mode("overwrite").option("header",True).csv(str(path))
def txt(c,default="Unknown",case=None):
    v=F.trim(F.coalesce(F.col(c),F.lit(""))); v=F.when(v=="",default).otherwise(v)
    return F.upper(v) if case=="upper" else F.lower(v) if case=="lower" else F.initcap(v) if case=="title" else v
def num(c,default=None):
    n=F.regexp_replace(F.regexp_replace(F.trim(F.coalesce(F.col(c),F.lit(""))),"\\$",""),",","").cast("double")
    return F.coalesce(n,F.lit(float(default))) if default is not None else n
def ts(c):
    v=F.trim(F.coalesce(F.col(c),F.lit(""))); return F.coalesce(F.to_timestamp(v,"yyyy-MM-dd HH:mm:ss"),F.to_timestamp(v,"MM/dd/yyyy HH:mm"))
def dt(c):
    v=F.trim(F.coalesce(F.col(c),F.lit(""))); return F.coalesce(F.to_date(v,"yyyy-MM-dd"),F.to_date(v,"MM/dd/yyyy"))

def main():
    s=spark()

    aircraft=(read(s,"master/aircraft.csv").withColumn("aircraft_id",txt("aircraft_id","", "upper")).withColumn("seat_capacity",num("seat_capacity",0))
              .withColumn("aircraft_status",txt("aircraft_status",case="title")).dropDuplicates(["aircraft_id"]))
    routes=(read(s,"reference/routes.csv").withColumn("route_id",txt("route_id","", "upper")).withColumn("origin_airport",txt("origin_airport","", "upper"))
            .withColumn("destination_airport",txt("destination_airport","", "upper")).withColumn("distance_miles",num("distance_miles",0)).dropDuplicates(["route_id"]))
    schedules=(read(s,"operations/flight_schedules.csv").withColumn("flight_id",txt("flight_id","", "upper")).withColumn("route_id",txt("route_id","", "upper"))
               .withColumn("aircraft_id",txt("aircraft_id","", "upper")).withColumn("scheduled_departure_ts",ts("scheduled_departure_ts"))
               .withColumn("scheduled_arrival_ts",ts("scheduled_arrival_ts")).withColumn("flight_date",F.to_date("scheduled_departure_ts"))
               .withColumn("flight_status",txt("flight_status",case="title")).dropDuplicates(["flight_id"]))
    events=(read(s,"operations/flight_events.csv").withColumn("flight_event_id",txt("flight_event_id","", "upper")).withColumn("flight_id",txt("flight_id","", "upper"))
            .withColumn("actual_departure_ts",ts("actual_departure_ts")).withColumn("actual_arrival_ts",ts("actual_arrival_ts"))
            .withColumn("departure_delay_minutes",num("departure_delay_minutes",0)).withColumn("arrival_delay_minutes",num("arrival_delay_minutes",0))
            .withColumn("operational_status",txt("operational_status",case="title")).dropDuplicates(["flight_event_id"]))
    bookings=(read(s,"booking/bookings.csv").withColumn("booking_id",txt("booking_id","", "upper")).withColumn("passenger_id",txt("passenger_id","", "upper"))
              .withColumn("booking_ts",ts("booking_ts")).withColumn("booking_status",txt("booking_status",case="title"))
              .withColumn("total_booking_amount",num("total_booking_amount")).dropDuplicates(["booking_id"]))
    segments=(read(s,"booking/booking_segments.csv").withColumn("booking_segment_id",txt("booking_segment_id","", "upper"))
              .withColumn("booking_id",txt("booking_id","", "upper")).withColumn("flight_id",txt("flight_id","", "upper"))
              .withColumn("base_fare",num("base_fare",0)).withColumn("tax_amount",num("tax_amount",0)).withColumn("ancillary_amount",num("ancillary_amount",0))
              .withColumn("segment_status",txt("segment_status",case="title")).dropDuplicates(["booking_segment_id"]))

    for n,df in {"aircraft":aircraft,"routes":routes,"flight_schedules":schedules,"flight_events":events,"bookings":bookings,"booking_segments":segments}.items(): write(df,SILVER/n)

    bad_aircraft=schedules.join(aircraft.select("aircraft_id"),"aircraft_id","left_anti")
    bad_route=schedules.join(routes.select("route_id"),"route_id","left_anti")
    bad_seg_flight=segments.join(schedules.select("flight_id"),"flight_id","left_anti")
    write(bad_aircraft,Q/"schedules_invalid_aircraft"); write(bad_route,Q/"schedules_invalid_route"); write(bad_seg_flight,Q/"segments_invalid_flight")

    flights=(schedules.join(events.select("flight_id","departure_delay_minutes","arrival_delay_minutes","actual_departure_ts","actual_arrival_ts"),"flight_id","left")
             .join(routes,"route_id","inner").join(aircraft.select("aircraft_id","seat_capacity"),"aircraft_id","inner")
             .withColumn("on_time_departure_flag",F.col("departure_delay_minutes")<=15).withColumn("long_delay_flag",F.col("departure_delay_minutes")>=60)
             .withColumn("cancelled_flag",F.col("flight_status")=="Cancelled"))
    bfact=(segments.join(bookings.select("booking_id","passenger_id","booking_ts","booking_channel","booking_status"),"booking_id","inner")
           .join(schedules.select("flight_id","route_id","aircraft_id","flight_date"),"flight_id","inner")
           .withColumn("segment_revenue",F.col("base_fare")+F.col("tax_amount")+F.col("ancillary_amount"))
           .withColumn("flown_flag",F.col("segment_status")=="Flown"))
    load=(bfact.groupBy("flight_id").agg(F.count("booking_segment_id").alias("booked_seats"),F.sum(F.col("flown_flag").cast("int")).alias("flown_passengers"),F.sum("segment_revenue").alias("passenger_revenue")))
    perf=(flights.join(load,"flight_id","left").fillna({"booked_seats":0,"flown_passengers":0,"passenger_revenue":0})
          .withColumn("load_factor",F.when(F.col("seat_capacity")>0,F.col("flown_passengers")/F.col("seat_capacity")).otherwise(0)))
    route_perf=(perf.groupBy("route_id","origin_airport","destination_airport").agg(F.count("flight_id").alias("flight_count"),
                F.sum(F.col("cancelled_flag").cast("int")).alias("cancelled_flights"),F.avg("departure_delay_minutes").alias("avg_departure_delay"),
                F.sum(F.col("on_time_departure_flag").cast("int")).alias("on_time_flights"),F.sum("flown_passengers").alias("passengers"),
                F.sum("passenger_revenue").alias("revenue"),F.avg("load_factor").alias("avg_load_factor"))
                .withColumn("on_time_rate",F.col("on_time_flights")/F.col("flight_count")))

    write(flights,GOLD/"fact_flights"); write(bfact,GOLD/"fact_booking_segments"); write(perf,GOLD/"mart_flight_performance"); write(route_perf,GOLD/"mart_route_performance")
    dq=s.createDataFrame([("schedules_invalid_aircraft",bad_aircraft.count()),("schedules_invalid_route",bad_route.count()),("segments_invalid_flight",bad_seg_flight.count())],["check_name","issue_count"])
    write(dq,REPORT/"data_quality_report")
    print("PySpark Aviation Medallion ETL complete."); s.stop()

if __name__=="__main__": main()
