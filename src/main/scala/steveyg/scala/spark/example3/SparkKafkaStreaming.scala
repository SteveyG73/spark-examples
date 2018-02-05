package steveyg.scala.spark.example3

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import steveyg.scala.spark.utils.SimpleJob
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkKafkaStreaming extends SimpleJob {

  if (args.length!=3) {
    println("Invalid number of arguments")
    System.exit(1)
  }

  val topic = args(0)
  val password = args(1)
  val outDir = args(2)
  val duration = 10

  val  conf = new SparkConf().setMaster("local[2]").setAppName("Streaming Example")

  val ssc = new StreamingContext(conf, Seconds(duration))

  val topics = Array(topic)


  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "ark-03.srvs.cloudkafka.com:9094,ark-01.srvs.cloudkafka.com:9094,ark-02.srvs.cloudkafka.com:9094",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "linux-laptop-stream-1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "security.protocol" -> "SSL",
    "ssl.truststore.location" -> "/var/private/ssl/client.truststore.jks",
    "ssl.truststore.password" -> password

  )

  val stream = KafkaUtils.createDirectStream[String,String] (
    ssc,
    PreferConsistent,
    Subscribe[String,String](topics,kafkaParams)
  )


  print("Records per second: 0")
  stream.foreachRDD(rdd => {
    val sql = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

    import sql.implicits._

    val df = rdd.map(rec => (rec.key(), rec.value())).toDF("key","value")

    val recordRate = df.count()/duration

    print("\b"*recordRate.toString.length + recordRate.toString)
    //TODO Implement write back to new topic

  })

  ssc.start()

  ssc.awaitTermination()
}
