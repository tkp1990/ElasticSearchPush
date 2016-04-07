package api.elasticsearch

import java.net.InetAddress
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory._
import play.api.libs.json.{JsObject, JsValue}

/**
 * Created by kenneththomas on 4/6/16.
 */
object ElasticsearchConfig {

  /**
   *
   * @param index - ElasticSearch index (if does not exists creates a new index)
   * @param clusterName - Elasticsearch cluster name (get health of ES will provide the cluster name)
   * @param home - path.home (where ES is installed) (usually /usr/share/elasticsearch)
   * @return - Returns ElasticSearch Client
   */
  def getClient(index: String, clusterName: String, home: String): Client = {
    val settings = Settings.settingsBuilder()
      .put("path.home", home)
      .put("cluster.name", clusterName)
      .put("action.bulk.compress", true)
      .build();
    val client = TransportClient.builder().settings(settings).build();
    client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300))
    val indexExists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!indexExists) {
      client.admin().indices().prepareCreate(index).execute().actionGet();
    }
    client
  }

  /**
   * !!!Implementation not completely provided!!! Currently just inserts List of Json Objects info ES
   *
   * Future Implementation - Error Handling, proper return types and other features
   *
   * @param jsonList - List of JSON objects which you want to insert into ES
   * @param client - ElasticSearch Client
   * @param index - ElasticSearch you want to add the data to
   * @param subIndex - the type of data being added (elasticSearch  Index/type)
   */
  def bulkInsert(jsonList: List[JsObject], client: Client, index: String, subIndex: String): Unit ={
    val bulkRequest = client.prepareBulk();
    for (x <- jsonList) {
      bulkRequest.add(client.prepareIndex(index, subIndex)
        .setSource(jsonBuilder().startObject().field(subIndex, x).endObject())
      );
    }
    val bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      // process failures by iterating through each bulk response item
    }
  }



}
