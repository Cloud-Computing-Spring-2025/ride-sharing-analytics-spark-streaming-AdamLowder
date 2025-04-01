from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

#initialize Spark session
spark = SparkSession.builder \
    .appName("RealTimeRideSharingAggregations") \
    .getOrCreate()

#define the schema for the incoming data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", FloatType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("timestamp", StringType(), True)
])

#define the socket source for streaming data
socket_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

#parse the JSON data from the stream
rides_df = socket_stream.selectExpr("CAST(value AS STRING)") \
    .selectExpr("from_json(value, 'trip_id STRING, driver_id INT, distance_km FLOAT, fare_amount FLOAT, timestamp STRING') as json_data") \
    .select("json_data.*")

#perform the real-time aggregation
aggregated_df = rides_df.groupBy("driver_id") \
    .agg(
        sum("fare_amount").alias("total_fare"),
        avg("distance_km").alias("avg_distance")
    )

#output the results to the console
query_console = aggregated_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

#store the result in CSV format with append mode
query_csv = aggregated_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/path/to/output/directory") \
    .option("checkpointLocation", "/path/to/checkpoint/directory") \
    .start()

#wait for the termination of the queries
query_console.awaitTermination()
query_csv.awaitTermination()