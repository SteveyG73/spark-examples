package steveyg.scala.spark.utils

import org.scalatest._



class KafkaConsumerClientTest extends FlatSpec with Matchers {

  "A Kafka consumer" should " pick up it's default consumer properties from a resources file" in {
    val brokerList = "blah:9094"
    val groupId = "nothing"
    val password = "test1234"
    val resourcesFileTest = KafkaConsumerClient.readProperties(brokerList,groupId,password)
    resourcesFileTest.getProperty("security.protocol") should not be null
  }

  it should " throw an exception if the credentials are invalid" in {
    val brokerList = "blah:9094"
    val groupId = "nothing"
    val password = "test1234"

    an [Exception] should be thrownBy {
      val badCreds = new KafkaConsumerClient(brokerList, groupId, password)
      badCreds.connect("test1,test2")
    }
  }

  it should " return some data from a valid broker with valid credentials" in {
    val brokerList = "localhost:9092"
    val groupId = "unit-testing"
    val password = ""
    val topic = "unit-testing"

    val getData = new KafkaConsumerClient(brokerList,groupId,password)

    getData.connect(topic)

    val records = getData.getMessageBatch

    records should not be empty
  }
}
