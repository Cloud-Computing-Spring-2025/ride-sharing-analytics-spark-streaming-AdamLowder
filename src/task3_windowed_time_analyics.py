from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window, sum

#initialize Spark session
spark = SparkSession.builder.appName("WindowedTimeAnalytics").getOrCreate()

#assuming you are reading a CSV file for initial data
parsed_df = spark.read.csv("path_to_your_data.csv", header=True, inferSchema=True)

#convert timestamp column to TimestampType
parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

#perform time-based aggregation
windowed_df = parsed_df.groupBy(window(col("event_time"), "5 minutes", "1 minute")) \
    .agg(sum("fare_amount").alias("total_fare"))

#write results to CSV
window_query = windowed_df.writeStream.outputMode("complete") \
    .format("csv") \
    .option("path", "output/time_windowed_aggregations") \
    .option("checkpointLocation", "output/checkpoints_window") \
    .start()

window_query.awaitTermination()