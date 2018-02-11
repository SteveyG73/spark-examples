package steveyg.scala.spark.utils

import org.scalatest.{FlatSpec, Matchers}

class KafkaProducerClientTest extends FlatSpec with Matchers {

  "A Kafka producer" should " pick up it's default consumer properties from a resources file" in {
    val brokerList = "blah:9094"
    val password = ""
    val resourcesFileTest = KafkaProducerClient.readProperties(brokerList,password)
    resourcesFileTest.getProperty("security.protocol") should not be null
  }

  it should " connect to a valid broker and send a message successfully" in {
    val brokerList = "localhost:9092"
    val password = ""
    val topic = "unit-testing"
    val sendData = new KafkaProducerClient(brokerList,topic, password)


    val dataMessage = List(("1","Hello World"))
    sendData.sendMessageBatch(dataMessage)

    sendData.disconnect
  }
}
