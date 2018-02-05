package steveyg.scala.spark.example1


import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import steveyg.scala.spark.utils.SimpleJob

// For implicit conversions from RDDs to DataFrames


object SparkWordCount extends SimpleJob {


  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.files.overwrite", "true")
    .getOrCreate()

  import spark.implicits._

  if (args.length != 2) {
    print("Must specify input file and output file")
    System.exit(1)
  }

  val inputFile = args(0)
  val outputFile = args(1)

  val df = spark.sparkContext
    .textFile(inputFile)
    .flatMap(_.split(" "))
    .filter(_.length > 1)
    .toDF("word")

  val result = df.groupBy("word")
    .count()
    .orderBy(desc("count"))

  writeToCSV(result, outputFile,"df")

  spark.close()
}
