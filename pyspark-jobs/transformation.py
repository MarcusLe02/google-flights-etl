from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, min as pyspark_min, avg as pyspark_avg
import sys

# Check input arguments
executionDate = ""
args = sys.argv[1:]

for i in range(0, len(args), 2):
    if args[i] == '--executionDate':
        executionDate = args[i + 1]

# Make sure the executionDate is provided
if not executionDate:
    raise ValueError("No executionDate provided. Use the --executionDate argument.")

runTime = executionDate.split("-")
year = runTime[0]
month = runTime[1]
day = runTime[2]

# Create Spark session
spark = SparkSession.builder.appName("Daily Report") \
        .config("hive.metastore.uris", "thrift://localhost:9083") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport().getOrCreate()

# Load data to Spark DataFrame, assuming the CSV file has a header row
flights_df = spark.read.option("header", "true").csv("hdfs://localhost:9000/datalake/orders")

# Add partition columns to the DataFrame if they don't already exist
flights_df = flights_df.withColumn("year", lit(year)) \
                        .withColumn("month", lit(month)) \
                        .withColumn("day", lit(day))

# Aggregate data
map_df = flights_df.groupBy("date_leave", "date_return", "companies", "scraping_date").agg(
    pyspark_min("price").alias("Minimum Price"),
    pyspark_avg("price").alias("Average Price")
)

# Write data to data warehouse
map_df.write.format("hive") \
        .partitionBy("year", "month", "day") \
        .mode("append") \
        .saveAsTable("daily_report")

spark.stop()
