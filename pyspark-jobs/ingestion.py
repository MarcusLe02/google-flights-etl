from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, max as pyspark_max
import sys
import os

# Check input arguments
if len(sys.argv) < 2:
    print("Yo, I need 2 arguments!")
    sys.exit(1)

tblName = ""
executionDate = ""
args = sys.argv[1:]

for i in range(0, len(args), 2):
    if args[i] == '--tblName':
        tblName = args[i + 1]
    elif args[i] == '--executionDate':
        executionDate = args[i + 1]

runTime = executionDate.split("-")
year = runTime[0]
month = runTime[1]
day = runTime[2]

# Create Spark session
spark = SparkSession.builder.appName("Ingestion - from MySQL to Hive") \
                            .config('spark.driver.extraClassPath', '/opt/homebrew/Cellar/apache-spark/3.5.0/libexec/jars/mysql-connector-j-8.2.0.jar').getOrCreate()

# Namenode and datanode configuration
namenodeHost = "namenode"
namenodePort = "9000"
tblLocation = f"hdfs://{namenodeHost}:{namenodePort}/datalake/{tblName}"

# Check if the table exists in the datalake
tblExists = os.path.exists(tblLocation)
tblQuery = f"(SELECT * FROM {tblName})"

if tblExists:
    df = spark.read.parquet(tblLocation)
    record_id = df.agg(pyspark_max("id")).head()[0]
    tblQuery = f"(SELECT * FROM {tblName} WHERE id > {record_id}) AS flights_incremental"

# MySQL configuration
mysqlHost = "mysql"
mysqlPort = "3307"
mysqlDatabase = "google_flight"
mysqlUser = "root"
mysqlPassword = "5nam"
mysqlUrl = f"jdbc:mysql://{mysqlHost}:{mysqlPort}/{mysqlDatabase}"

# Read from MySQL
jdbcDf = spark.read.format("jdbc").options(
    url=mysqlUrl,
    driver="com.mysql.cj.jdbc.Driver",
    dbtable=tblQuery,
    user=mysqlUser,
    password=mysqlPassword,
    fetchSize="1000"
).load()

# Write to HDFS
jdbcDf.withColumn("year", lit(year)) \
    .withColumn("month", lit(month)) \
    .withColumn("day", lit(day)) \
    .write \
    .partitionBy("year", "month", "day") \
    .mode("append") \
    .parquet(tblLocation)

spark.stop()