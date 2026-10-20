from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.table
def one_big_ride_table():
    df_ride= spark.readStream.table("uber_project.silver.final_ride_data")
    df_cities= spark.table("uber_project.bronze.map_cities")
    df_payment= spark.table("uber_project.bronze.map_payment_methods")
    df_ride_status= spark.table("uber_project.bronze.map_ride_statuses")
    df_cancel= spark.table("uber_project.bronze.map_cancellation_reasons")
    df_Vehicle= spark.table("uber_project.bronze.map_vehicle_types")

    df_obt= df_ride.join(df_cities,df_cities.city_id==df_ride.pickup_city_id,"left")\
                .join(df_payment,df_payment.payment_method_id==df_ride.payment_method_id,"left")\
                    .join(df_ride_status,df_ride_status.ride_status_id==df_ride.ride_status_id,"left")\
                        .join(df_cancel,df_cancel.cancellation_reason_id==df_ride.cancellation_reason_id,"left")\
                            .join(df_Vehicle,df_Vehicle.vehicle_type_id==df_ride.vehicle_type_id,"left")\
                                .select(df_ride["*"],df_payment.is_card,df_payment.payment_method,df_payment.requires_auth,df_cities.city,df_cities.region,df_cities.state,df_ride_status.is_completed,df_ride_status.ride_status,df_cancel.cancellation_reason,df_Vehicle.base_rate,df_Vehicle.description,df_Vehicle.per_mile,df_Vehicle.per_minute,df_Vehicle.vehicle_type)
    
    return df_obt
