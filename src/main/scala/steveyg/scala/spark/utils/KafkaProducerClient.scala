package steveyg.scala.spark.utils

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducerClient {
  def readProperties(brokerList: String, password: String): Properties = {

    val resourceProperties: Properties = new Properties()
    val fileStream = getClass.getResource("/producer.properties").openStream()

    resourceProperties.load(fileStream)

    resourceProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    resourceProperties.put("ssl.truststore.password", password)

    resourceProperties

  }
}

import steveyg.scala.spark.utils.KafkaProducerClient._

class KafkaProducerClient(brokerList: String, topic: String, password: String) {

  val producerProperties: Properties = readProperties(brokerList, password)
  val producer = new KafkaProducer[String, String](producerProperties)


  def sendMessageBatch(records: List[(String, String)]): Unit = {
    records.map(r => producer.send(new ProducerRecord[String, String](topic, r._1, r._2)))

  }

  def disconnect: Unit = {
    producer.close()
  }


}


