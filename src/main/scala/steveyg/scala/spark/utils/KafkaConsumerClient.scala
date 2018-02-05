package steveyg.scala.spark.utils

import java.util.Properties

import collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}

object KafkaConsumerClient {
  def readProperties(brokerList: String, groupId: String, password: String): Properties = {

    val resourceProperties: Properties = new Properties()
    val fileStream = getClass.getResource("/consumer.properties").openStream()

    resourceProperties.load(fileStream)

    resourceProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    resourceProperties.put("group.id", groupId)
    resourceProperties.put("ssl.truststore.password", password)

    resourceProperties

  }
}

import KafkaConsumerClient._

class KafkaConsumerClient(brokerList: String, groupId: String, password: String) {

  val consumerProperties = readProperties(brokerList, groupId, password)
  val consumer = new KafkaConsumer[String,String](consumerProperties)

  def createTopicList(topics: String) : List[String] = {
    val topicList = topics.split(',').map(t => t.trim).toList

    topicList
  }

  def connect(topics: String): Unit = {
    consumer.subscribe(createTopicList(topics).asJava)

//    throw new Exception(" Cannot connect")

  }

  def getMessageBatch : List[(String,String)] = {
    val records : ConsumerRecords[String,String] = consumer.poll(10000)

    records.asScala.toList.map(r => (r.key(),r.value()))

  }

}


