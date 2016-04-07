package mongoToEs

import api.elasticsearch.ElasticsearchConfig
import com.mongodb.casbah.Cursor
import com.mongodb.casbah.Imports._
import api.mongo.MongoConfig
import play.api.libs.json.{Json, JsValue, JsObject}

/**
 * Created by kenneththomas on 4/7/16.
 */
class PushPartialData {

  val orderBy = MongoDBObject("_id" -> 1)
  val INDEX = "supplier2"
  val SUB_INDEX = "data"
  val HOME_PATH = """/usr/share/elasticsearch"""
  val CLUSTER_NAME = """elasticsearch"""

  def getData() = {
    val finalCount = 52982819
    var skip, c = 0
    val limit = 10000
    var lastId = ""

    while (finalCount >= skip ) {
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      try {
        val collection = MongoConfig.getCollection("myDb", "myCollection1", mongoClient)
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
          println(lastId)
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
      println(json)
      last_id = (json \ "_id" ).as[String].trim
      //println("id" + last_id)
      val suppData = (json \ "value").as[JsValue]
      val supName = (suppData \ "").as[String]
      val supAddr = (suppData \ "").as[String]
      val conName = (suppData \ "").as[String]
      val conAddr = (suppData \ "").as[String]
      val n1Name = (suppData \ "").as[String]
      val n1Addr = (suppData \ "").as[String]
      val n2Name = (suppData \ "").as[String]
      val n2Addr = (suppData \ "").as[String]
      val obj = Json.obj("id" -> last_id, "supname" -> supName, "supAddr" -> supAddr, "conname" -> conName, "conAddr" -> conAddr,
        "n1name" -> n1Name, "n1Addr" -> n1Addr, "n2name" -> n2Name, "n2Addr" -> n2Addr)
      jsList = obj :: jsList
    }
    val client = ElasticsearchConfig.getClient(INDEX, HOME_PATH, CLUSTER_NAME)
    try{
      ElasticsearchConfig.bulkInsert(jsList, client, INDEX, SUB_INDEX)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      client.close()
    }
    last_id
  }
}