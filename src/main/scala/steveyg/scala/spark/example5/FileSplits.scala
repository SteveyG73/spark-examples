package steveyg.scala.spark.example5

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.sql.DriverManager

object FileSplits extends App {

  val conf: SparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("File splits")
    .setExecutorEnv("spark.executor.memory","512M")


  val spark = SparkSession.builder()
    .appName("Spark JSON log file reader")
    .config(conf)
    .getOrCreate()

  val sql = spark.sqlContext
  val postgresUrl = "jdbc:postgresql:landreg"
  val query = "(select * from land_registry_price_paid_uk where postcode like 'CM2%') AS t"
  val connectionProperties = new Properties
  val connConf = Map("driver"->"org.postgresql.Driver","user"->"lreg_read","password"->"lreg_read")
  connectionProperties.put("driver","org.postgresql.Driver")
  connectionProperties.put("user","lreg_read")
  connectionProperties.put("password","lreg_read")

  val landRegEntries = sql.read.jdbc(postgresUrl,query,connectionProperties)

  landRegEntries.printSchema()
  val avgByStreet = landRegEntries.groupBy("street").avg("price").orderBy("street")
  avgByStreet.coalesce(1).write.option("header","true").csv("/home/dad/street_prices")

}
