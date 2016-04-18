package mongoToEs

import java.net.InetAddress

import api.elasticsearch.ElasticsearchConfig
import com.mongodb.casbah.Cursor
import com.mongodb.casbah.Imports._
import api.mongo.MongoConfig
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import play.api.libs.json.{Json, JsValue, JsObject}

/**
 * Created by kenneththomas on 4/7/16.
 */
class PushPartialData {

  val orderBy = MongoDBObject("_id" -> 1)
  val INDEX = "supplier2"
  val SUB_INDEX = "data"
  val HOME_PATH = """/usr/share/elasticsearch"""
  val CLUSTER_NAME = "elasticsearch"

  def getData() = {
    val finalCount = 52982819
    var skip, c = 0
    val limit = 10000
    var lastId = ""

    while (finalCount >= skip ) {
      println(" Count: "+skip)
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      try {
        val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
        if(skip == c) {
          val data = collection.find().skip(skip).limit(limit).sort(orderBy)
          skip = skip + limit
          lastId = submitToEs(data)
          println(lastId)
        } else {
          val q = "_id" $gt (lastId)
          val data = collection.find(q).limit(limit)
          skip = skip + limit
          lastId = submitToEs(data)
          println("Id: "+lastId)
        }
      } catch {
        case e: Exception => println("Exception getData/PushPartialData/getData() " + e.getMessage)
          e.printStackTrace()
      } finally {
        mongoClient.close()
      }
    }
  }

  def submitToEs(data: MongoCursor): String = {
    var last_id = ""
    var jsList: List[JsObject] = List[JsObject]()
    for(x <- data) {
      val json = Json.parse(x.toString);
      //println(json)
      last_id = (json \ "_id" ).as[String].trim
      //println("id" + last_id)
      val suppData = (json \ "value").as[JsValue]
      val supName = (suppData \ "supname").as[String]
      val supAddr = (suppData \ "supaddr").as[String]
      val conName = (suppData \ "conname").as[String]
      val conAddr = (suppData \ "conaddr").as[String]
      val n1Name = (suppData \ "n1name").as[String]
      val n1Addr = (suppData \ "n1addr").as[String]
      val n2Name = (suppData \ "n2name").as[String]
      val n2Addr = (suppData \ "n2addr").as[String]
      val prcn = (suppData \ "prcn").as[String]
      val obj = Json.obj("id" -> last_id, "supname" -> supName, "supAddr" -> supAddr, "conname" -> conName, "conAddr" -> conAddr,
        "n1name" -> n1Name, "n1Addr" -> n1Addr, "n2name" -> n2Name, "n2Addr" -> n2Addr)
      jsList = obj :: jsList
    }
    val client = getClient(INDEX)
    try{
      ElasticsearchConfig.bulkInsert(jsList, client, INDEX, SUB_INDEX)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      client.close()
    }
    last_id
  }

  def getClient(index: String): Client = {
    val settings = Settings.settingsBuilder()
      .put("path.home", "/usr/share/elasticsearch")
      .put("cluster.name", "elasticsearch")
      .put("action.bulk.compress", true)
      .build();
    //val node = nodeBuilder().local(true).settings(settings).node();
    val client = TransportClient.builder().settings(settings).build();
    client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300))
    /*val client = NodeBuilder.nodeBuilder()
      .client(true)
      .node()
      .client();*/
    val indexExists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!indexExists) {
      client.admin().indices().prepareCreate(index).execute().actionGet();
    }
    client
  }
}