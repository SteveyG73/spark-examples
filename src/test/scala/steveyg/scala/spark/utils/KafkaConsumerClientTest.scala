package steveyg.scala.spark.utils

import org.scalatest._

class KafkaConsumerClientTest extends FlatSpec with Matchers {


  "A Kafka consumer" should " pick up it's default consumer properties from a resources file" in {
    val brokerList = "blah:9094"
    val groupId = "nothing"
    val password = "test1234"
    val resourcesFileTest = KafkaConsumerClient.readProperties(brokerList,groupId,password)
    resourcesFileTest.getProperty("") should not be null
  }

  it should " throw an exception if the credentials are invalid" in {
    val brokerList = "blah:9094"
    val groupId = "nothing"
    val password = "test1234"

    a [Exception] should be thrownBy {
      val badCreds = new KafkaConsumerClient(brokerList, groupId, password)
    }
  }
}
