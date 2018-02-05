package steveyg.scala.spark.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

object KafkaConsumerClient {
  def readProperties(brokerList: String, groupId: String, password: String) : Properties = {

    val resourceProperties : Properties = new Properties()
    resourceProperties.load(getClass.getResource("consumer.properties").openStream())

    resourceProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    resourceProperties.put("group.id", groupId)
    resourceProperties.put("ssl.truststore.password", password)

    resourceProperties

  }
}

import KafkaConsumerClient._

class KafkaConsumerClient(brokerList: String, groupId: String, password: String) {

  val consumerProperties = readProperties(brokerList, groupId, password)

}


