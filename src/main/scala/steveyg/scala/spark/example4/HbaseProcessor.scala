package steveyg.scala.spark.example4

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.hbase._


object HbaseProcessor extends App {

    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HBASE_Processor")
      .setExecutorEnv("spark.executor.memory","512M")
      .set("spark.hbase.host", "localhost")

    val hbaseConf = "/opt/hbase/hbase-1.4.1/conf/hbase-site.xml"

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    val sql = spark.sqlContext

    val catEntries =
      s"""
         |{
         |"table":{"namespace":"landreg", "name":"entries", "tableCoder":"PrimitiveType"},
         |"rowkey":"key",
         |"columns":{
         |  "pk":{"cf":"rowkey","col":"key", "type":"string"},
         |  "postcode":{"cf":"atts", "col":"postcode", "type":"string"},
         |  "price":{"cf":"values","col":"price", "type":"string"}
         | }
         |}
     """.stripMargin

    val catPostcodes =
        s"""
           |{
           |"table":{"namespace":"landreg", "name":"postcodes", "tableCoder":"PrimitiveType"},
           |"rowkey":"key",
           |"columns":{
           |  "pk":{"cf":"rowkey","col":"key", "type":"string"},
           |  "postcode":{"cf":"address", "col":"county", "type":"string"},
           |  "latitude":{"cf":"coords","col":"lat", "type":"string"},
           |  "longitude":{"cf":"coords","col":"long", "type":"string"}
           | }
           |}
     """.stripMargin

    def withCatalogue(catalogue: String, conf: String): DataFrame = {
      sql
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> catalogue,
                     HBaseRelation.HBASE_CONFIGFILE -> conf))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()
    }

    val landregEntries = withCatalogue(catEntries,hbaseConf)
    val landregPostcodes = withCatalogue(catPostcodes,hbaseConf)
    println(landregEntries.take(1).mkString(","))
    println(landregPostcodes.take(1).mkString(","))

    val joined = landregEntries.as("e").join(landregPostcodes.as("p"),landregEntries("postcode")===landregPostcodes("postcode"))

    println(joined.take(10).mkString(","))

    spark.stop()

}
