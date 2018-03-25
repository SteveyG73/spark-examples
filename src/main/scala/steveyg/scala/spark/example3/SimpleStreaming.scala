package steveyg.scala.spark.example3

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, ProcessingTime}
import scala.concurrent.duration._

object SimpleStreaming extends App {

  val duration = 10

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Simple Streaming Example").setExecutorEnv("spark.executor.memory","512M")

  val spark = SparkSession.builder().config(conf).getOrCreate()

  val events = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stream1")
    .option("startingoffsets", "latest")
    .load
    .withColumn("duration", lit(duration)).toDF()


  val results =  events.groupBy("duration").agg(count(lit(1)).alias("record_count")).selectExpr("record_count/duration as rps")

  results.writeStream
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime(duration.seconds))
    .outputMode(OutputMode.Complete())
    .queryName("from-kafka-to-console")
    .start

  spark.streams.awaitAnyTermination()

}
