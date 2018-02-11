package steveyg.scala.spark.utils

import com.sksamuel.elastic4s.{ElasticsearchClientUri, RefreshPolicy}
import com.sksamuel.elastic4s.http.{ElasticDsl, HttpClient}


class ElasticWriterClient extends ElasticDsl{

  def connect(elasticURL: String, elasticPort: Int) : HttpClient  = {
      HttpClient(ElasticsearchClientUri(elasticURL, elasticPort))
  }

  def updateBatch(client: HttpClient, records: Seq[(String,String)]) : Unit = {
    client.execute {
      bulk(
        indexInto("metrics").fields(records)
      ).refresh(RefreshPolicy.WAIT_UNTIL)
    }
  }

  def disconnect(client: HttpClient) : Unit = {
    client.close()
  }
}
