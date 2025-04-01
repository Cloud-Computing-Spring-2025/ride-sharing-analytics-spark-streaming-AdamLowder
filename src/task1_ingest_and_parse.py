from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

#create a spark session
spark = SparkSession.builder.appName("RideSharingStreaming").getOrCreate()

#define schema for incoming JSON
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", IntegerType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

#read from socket
raw_stream = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

#parse JSON messages
parsed_df = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

#print parsed data to console
query = parsed_df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()