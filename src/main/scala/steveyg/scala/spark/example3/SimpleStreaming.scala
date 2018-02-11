package steveyg.scala.spark.example3

import java.util.Calendar

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{Trigger, OutputMode, ProcessingTime}
import scala.concurrent.duration._

object SimpleStreaming extends App {

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Simple Streaming Example").setExecutorEnv("spark.executor.memory","512M")

  val spark = SparkSession.builder().config(conf).getOrCreate()

  val events = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "unit-testing")
    .option("startingoffsets", "latest")
    .option("maxOffsetsPerTrigger", 1)
    .load


  val results = events.
    select(
      events("key") cast "string",   // deserialize keys
      events("value") cast "string", // deserialize values
      events("topic"),
      events("partition"),
      events("offset"))

  results.writeStream
    .format("console")
    .option("truncate", false)
    .trigger(Trigger.ProcessingTime(10.seconds))
    .outputMode(OutputMode.Append)
    .queryName("from-kafka-to-console")
    .start

  spark.streams.awaitAnyTermination()

}
