# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC select * from uber_project.silver.one_big_ride_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select passenger_id, passenger_name, passenger_email,passenger_phone from uber_project.silver.one_big_ride_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_id,driver_name,driver_phone,driver_license,driver_rating from uber_project.silver.one_big_ride_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select ride_id,ride_status,passenger_id,driver_id, vehicle_id ,confirmation_number, pickup_address, dropoff_address, base_fare,distance_fare,time_fare,tip_amount,total_fare from uber_project.silver.one_big_ride_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select vehicle_id,vehicle_type,vehicle_type_id,vehicle_make_id,vehicle_model,vehicle_color from uber_project.silver.one_big_ride_table

# COMMAND ----------

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

    target= "dim_Passenger"
    source= "Passeneger_View"
    keys= "passenger_id",
    sequence_by= "passenger_id",
    stored_as_scd_type= 1
)

