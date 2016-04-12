package mongoToEs

import java.net.InetAddress

import api.mongo.MongoConfig
import com.mongodb.casbah.Imports._
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.node.NodeBuilder
import play.api.libs.json.{JsValue, Json}
import tokenize.GetZPMainData

/**
 * Created by kenneththomas on 4/11/16.
 *
 */
class PushTokenizedToES {

  val ID = "_id"
  val orderBy = MongoDBObject(ID -> 1)
  val INDEX = "tokenized"
  val DB = "datacleaning"
  val COLLECTION = "ZPmainCollection"


  def getData() = {
    val finalCount = getCount()
    var skip, c = 0
    val limit = 1000
    var last_id = ""
    while (finalCount >= skip) {
      println(" Count: "+skip)
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      try {
        val collection = MongoConfig.getCollection(DB, COLLECTION, mongoClient)
        var jsList: List[JsValue] = List[JsValue]()
        if(skip == c){
          val data = collection.find().skip(skip).limit(limit).sort(orderBy)
          skip = skip + limit
          for(x <- data) {
            val json = Json.parse(x.toString);
            last_id = (json \ ID ).as[String].trim
            //println("id" + last_id)
            val supplier = (json \ "tokenized").as[JsValue]
            jsList = supplier :: jsList
          }
          println("id" + last_id)
          val client = getClient(INDEX)
          try{
            bulkInsert(jsList, client)
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            client.close()
          }
        } else {
          val q = ID $gt (last_id)
          val data = collection.find(q).limit(limit)
          skip = skip + limit
          val tempId = last_id
          for(x <- data) {
            val json = Json.parse(x.toString);
            last_id = (json \ ID ).as[String].trim
            try{
              val supplier = (json \ "tokenized").as[JsValue]
              jsList = supplier :: jsList
            } catch {
              case e: Exception =>
                println("Exception: " + e.getMessage)

                val tokenizedObj = new GetZPMainData
                tokenizedObj.tokenizeAndSave(last_id, 1000, DB, COLLECTION)
                val q = ID $gt (last_id)
                val data = collection.find(q).limit(limit)
                for(x <- data) {
                  val json = Json.parse(x.toString);
                  last_id = (json \ ID).as[String].trim
                  val supplier = (json \ "tokenized").as[JsValue]
                  jsList = supplier :: jsList
                }
            }
            println("id" + last_id)
          }
          val client = getClient(INDEX)
          try{
            if(jsList.length > 0){
              bulkInsert(jsList, client)
            } else {
              println("Search Results is empty, Noting to insert")
            }
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            client.close()
          }
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
      } finally {
        mongoClient.close()
      }

    }
  }

  /**
   *
   * @param index - ES Index
   * @return - ElasticSearch Client
   *
   */
  def getClient(index: String): Client = {
    val settings = Settings.settingsBuilder()
      .put("path.home", "/usr/share/elasticsearch")
      .put("cluster.name", "elasticsearch")
      .put("action.bulk.compress", true)
      .build();
    //val node = nodeBuilder().local(true).settings(settings).node();
    val client = TransportClient.builder().settings(settings).build();
    client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300))
    val indexExists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!indexExists) {
      client.admin().indices().prepareCreate(index).execute().actionGet();
    }
    client
  }

  def bulkInsert(jsonList: List[JsValue], client: Client): Unit ={
    val bulkRequest = client.prepareBulk();
    for (x <- jsonList) {
      bulkRequest.add(client.prepareIndex(INDEX, "data")
        .setSource(jsonBuilder().startObject().field("data", x).endObject())
      );
    }
    val bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      // process failures by iterating through each bulk response item
    }
  }

  /**
   *
   * @return - an Integer value which represent the count of number of records present in a the Collection ZPmainCollection
   *
   */
  def getCount(): Integer = {
    var count = 0
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      count = collection.count()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      mongoClient.close()
    }
    count
  }
}

