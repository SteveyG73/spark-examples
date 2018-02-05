package steveyg.scala.spark.example2

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.io.File
import java.io.PrintWriter
import steveyg.scala.spark.utils.SimpleJob


object SparkJSONLogProcessor extends SimpleJob {




  val spark = SparkSession.builder()
    .appName("Spark JSON log file reader")
    .getOrCreate()

  if (args.length != 2) {
    print("Must specify input file and output file")
    System.exit(1)
  }

  val inFile = args(0)
  val outFile = args(1)

  val df = spark
    .read
    .json(inFile)


  val df2=   df.withColumn("timestamp",to_timestamp(df("time"),"dd/MMM/yyyy:HH:mm:ss Z"))

  writeToCSV(df2,outFile,"df2")

  val schemaDef = df2.schema.treeString
  val outSchemaFile = outFile+"/"+"schema.txt"
  val writer = new PrintWriter(new File(outSchemaFile))
  writer.write(schemaDef)
  writer.close()

  df2.createOrReplaceTempView("logs")

  val df3 = spark
    .sql(
      """
        | select year(timestamp) as year,
        |        month(timestamp) as month,
        |        day(timestamp) as day,
        |        count(*) as log_count
        | from logs
        | group by year(timestamp), month(timestamp), day(timestamp)
      """.stripMargin('|'))

  writeToCSV(df3,outFile,"df3")

  spark.close()
}
