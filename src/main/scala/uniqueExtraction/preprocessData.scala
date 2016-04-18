package uniqueExtraction

import api.mongo.MongoConfig
import com.mongodb.casbah.Imports._
import play.api.libs.json.{JsValue, Json}

import scala.collection.immutable.HashMap

/**
 * Created by kenneththomas on 4/18/16.
 */
class preprocessData {

  val businessTypeMap: HashMap[String, String] = HashMap("ltd" -> "LTD", "limited" -> "Limited", "pvt ltd" -> "Private Limited",
    "india pvt ltd" -> "India Private Limited", "llp" -> "LLP", "inc" -> "Inc", "co" -> "cooperated","corp" -> "cooperated", "llc" -> "LLC",
    "limitada" -> "Limited","co limit" -> "Co Limited", "co limited" -> "Co Limited", "co ltd" -> "Co Limited", "pty ltd" -> "Pty Limited",
    "s a " -> "SA", "sa" -> "SA")

  def getData() = {
    val finalCount = 52982819
    var skip, c = 0
    val limit = 100
    var lastId = ""

    while (finalCount >= skip ) {
      println(" Count: "+skip)
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      try{
        val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
        if(skip == c) {
          val data = collection.find().skip(skip).limit(limit).sort(Constants.orderBy)
          skip = skip + limit
          //lastId = processSequentially(data)
          println("Last Id "+lastId)
        } else {
          val q = "_id" $gt (lastId)
          val data = collection.find(q).limit(limit)
          skip = skip + limit
          //lastId = processSequentially(data)
          println("Last Id: "+lastId)
        }
      } catch {
        case e: Exception =>
      } finally {
        mongoClient.close()
      }
    }
  }

  def preProcessData(data: MongoCursor): String = {
    var lastId = ""
    var collList = List.empty
    for(x <- data) {
      val json = Json.parse(x.toString);
      lastId = (json \ "_id" ).as[String].trim
      val suppData = (json \ "value").as[JsValue]
      val supName = (suppData \ "supname").as[String]
      val conName = (suppData \ "conname").as[String]
      val n1Name = (suppData \ "n1name").as[String]
      val n2Name = (suppData \ "n2name").as[String]
    }
    lastId
  }

  def confirmIndex(collection: MongoCollection) = {
    val index = collection.getIndexInfo.exists(x => x.getAs[String]("name").getOrElse("") == Constants.NAME_UNIQUE_INDEX)
    if(index){
      println("Index already Exists")
    } else {
      collection.createIndex("p_text", Constants.NAME_UNIQUE_INDEX, true)
    }
  }

}


