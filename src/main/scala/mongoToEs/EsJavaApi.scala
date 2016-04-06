package mongoToEs

import com.mongodb.casbah.Imports._
import org.elasticsearch.client.Client
import org.elasticsearch.node.NodeBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import play.api.libs.json.{JsValue, Json, JsObject}
;

/**
 * Created by kenneththomas on 4/6/16.
 */
class EsJavaApi {

  val INDEX = "supplier"

  def getData() = {
    val finalCount = 52982819
    var skip = 0
    val limit = 5000
    while (finalCount >= skip ) {
      val mongoClient = getMongoClient("localhost", 27017)
      val (collection, mdbClient) = getCollection("datacleaning", "ZPmainCollection", mongoClient)
      try {
        val data = collection.find().skip(skip).limit(limit)
        skip = skip + limit
        var jsonList: List[JsObject] = List[JsObject]()
        for(x <- data) {
          val json = Json.parse(x.toString);
          val supplier = (json \ "value").as[JsValue]
          val jObj = Json.obj("data" -> supplier)
          //println(json.toString())
          jsonList = jObj :: jsonList
        }
        val client = getClient(INDEX)
        try{
          bulkInsert(jsonList, client)
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          client.close()
        }
      } catch {
        case e: Exception => println("Exception: "+ e.getMessage)
      } finally {
        mdbClient.close()
      }
    }
  }

  def getClient(index: String): Client = {
    val client = NodeBuilder.nodeBuilder()
      .client(true)
      .node()
      .client();
    val indexExists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!indexExists) {
      client.admin().indices().prepareCreate(index).execute().actionGet();
    }
    client
  }

  def bulkInsert(jsonList: List[JsObject], client: Client): Unit ={
    val bulkRequest = client.prepareBulk();
    for (x <- jsonList) {
      bulkRequest.add(client.prepareIndex(INDEX, "data")
        .setSource(x)
      );
    }
    val bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      // process failures by iterating through each bulk response item
    }
  }

  def getCollection(_db: String, _collection: String, mongoClient: MongoClient): (MongoCollection, MongoClient) = {
    val db = mongoClient(_db)
    val collection = db(_collection)
    (collection, mongoClient)
  }

  def getMongoClient(host: String, port: Int): MongoClient = {
    MongoClient(host, port)
  }

}
