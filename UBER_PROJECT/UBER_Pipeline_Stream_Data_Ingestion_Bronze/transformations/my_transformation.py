from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import pipelines as dp

# Event Hubs configuration
EH_NAMESPACE = "Vivo-EventHub"
EH_NAME = "uber_evenhub"

EH_CONN_STR = spark.conf.get("Connection_String")
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 60000,
  "kafka.session.timeout.ms" : 30000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : "true",
  "startingOffsets"          : "earliest"
}

@dp.table
def Rides_raw():
  df= spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()

  df= df.withColumn("Rides", col("value").cast("string"))
  return df















