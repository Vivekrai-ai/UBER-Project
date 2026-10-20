from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp


@dp.view
def Passenger_View():
    df= spark.readStream.table("uber_project.silver.one_big_ride_table")
    df= df.select("passenger_id", "passenger_name", "passenger_email","passenger_phone")
    df= df.dropDuplicates(["passenger_id"])
    return df


dp.create_streaming_table("dim_Passenger")

dp.create_auto_cdc_flow(

    target= "dim_Passenger",
    source= "Passenger_View",
    keys= ["passenger_id"],
    sequence_by= "passenger_id",
    stored_as_scd_type= 1
)

@dp.view
def Driver_View():
    df= spark.readStream.table("uber_project.silver.one_big_ride_table")
    df= df.select("driver_id","driver_name","driver_phone","driver_license","driver_rating")
    df= df.dropDuplicates(["driver_id"])
    return df


dp.create_streaming_table("dim_Driver")

dp.create_auto_cdc_flow(

    target= "dim_Driver",
    source= "Driver_View",
    keys= ["driver_id"],
    sequence_by= "driver_id",
    stored_as_scd_type= 1
)

@dp.view
def Vehicle_View():
    df= spark.readStream.table("uber_project.silver.one_big_ride_table")
    df= df.select("vehicle_id","vehicle_type","vehicle_type_id","vehicle_make_id","vehicle_model","vehicle_color")
    df= df.dropDuplicates(["vehicle_id"])
    return df


dp.create_streaming_table("dim_Vehicle")

dp.create_auto_cdc_flow(

    target= "dim_Vehicle",
    source= "Vehicle_View",
    keys= ["vehicle_id"],
    sequence_by= "vehicle_id",
    stored_as_scd_type= 1
)

@dp.view
def Ride_View():
    df= spark.readStream.table("uber_project.silver.one_big_ride_table")
    df= df.select("ride_id","ride_status","passenger_id","driver_id", "vehicle_id" ,"confirmation_number", "pickup_address", "dropoff_address","base_fare","distance_fare","time_fare","tip_amount","total_fare")
    df= df.dropDuplicates(["ride_id"])
    return df


dp.create_streaming_table("Fact_Ride")

dp.create_auto_cdc_flow(

    target= "Fact_Ride",
    source= "Ride_View",
    keys= ["ride_id"],
    sequence_by= "ride_id",
    stored_as_scd_type= 1
)





















