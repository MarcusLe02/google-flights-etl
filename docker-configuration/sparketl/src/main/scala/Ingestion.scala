import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{lit, max}

object Ingestion {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Yo, I need two arguments!")
    }
    
    var tblName = ""
    var executionDate = ""
    args.sliding(2, 2).toList.collect {
      case Array("--tblName", argTblName: String) => tblName = argTblName
      case Array("--executionDate", argExecutionDate: String) => executionDate = argExecutionDate
    }

    val runTime = executionDate.split("-")
    val year = runTime(0)
    val month = runTime(1)
    val day = runTime(2)

    // create spark session
    val spark = SparkSession
      .builder()
      .appName("Ingestion - from MYSQL to HIVE")
      .getOrCreate()

    // Assuming namenode and datanode services are up and running as per the docker-compose file
    val namenodeHost = "namenode"
    val namenodePort = "9000"
    val tblLocation = s"hdfs://$namenodeHost:$namenodePort/datalake/$tblName"
    
    // Check if the table exists in the datalake
    val tblExists = spark.sparkContext.hadoopConfiguration
      .asInstanceOf[org.apache.hadoop.conf.Configuration]
      .filesystem.exists(new org.apache.hadoop.fs.Path(tblLocation))

    var tblQuery = s"(SELECT * FROM $tblName) AS flights_data" // Initial query to load all data

    if (tblExists) {
      val df = spark.read.parquet(tblLocation)
      val record_id = df.agg(max("id")).head().getLong(0)
      tblQuery = s"(SELECT * FROM $tblName WHERE id > $record_id) AS flights_incremental"
    }

    // MySQL configuration
    val mysqlHost = "mysql" // Docker service name for MySQL
    val mysqlPort = "3307" // Port specified in docker-compose
    val mysqlDatabase = "google_flight"
    val mysqlUser = "root"
    val mysqlPassword = "5nam"
    val mysqlUrl = s"jdbc:mysql://$mysqlHost:$mysqlPort/$mysqlDatabase"

    // Read from MySQL
    val jdbcDF = spark.read.format("jdbc").options(
      Map(
        "url" -> mysqlUrl,
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "dbtable" -> tblQuery,
        "user" -> mysqlUser,
        "password" -> mysqlPassword,
        "fetchSize" -> "1000"
      )
    ).load()

    // Write to HDFS
    jdbcDF.withColumn("year", lit(year))
          .withColumn("month", lit(month))
          .withColumn("day", lit(day))
          .write
          .partitionBy("year", "month", "day")
          .mode(SaveMode.Append)
          .parquet(tblLocation)

    spark.stop()
  }
}