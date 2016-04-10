package tokenize

import api.elasticsearch.ElasticsearchConfig
import api.mongo.MongoConfig
import com.mongodb.casbah.Imports
import com.mongodb.casbah.Imports._
import play.api.libs.json._

/**
 * Created by kenneththomas on 4/10/16.
 */
class GetZPMainData {

  val orderBy = MongoDBObject("_id" -> 1)

  implicit val idReads: Reads[ObjectId] = new Reads[ObjectId] {
    override def reads(json: JsValue): JsResult[Imports.ObjectId] = {
      json.asOpt[String] map { str =>
        if (org.bson.types.ObjectId.isValid(str))
          JsSuccess(new ObjectId(str))
        else
          JsError("Invalid ObjectId %s".format(str))
      } getOrElse (JsError("Value is not an ObjectId"))
    }
  }
  implicit val idWrites = new Writes[ObjectId] {
    def writes(oId: ObjectId): JsValue = {
      JsString(oId.toString)
    }
  }

  def getData () = {
    val finalCount = 10//52982819
    var skip, c = 0
    val limit = 1//10000
    var lastId = ""

    while (finalCount >= skip ) {
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      try {
        val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
        if(skip == c) {
          val data = collection.find().skip(skip).limit(limit).sort(orderBy)
          skip = skip + limit
          lastId = processBatch(data)
          println(lastId)
        } else {
          val q = "_id" $gt (lastId)
          val data = collection.find(q).limit(limit)
          skip = skip + limit
          lastId = processBatch(data)
          println("Id: "+lastId)
        }
      } catch {
        case e: Exception =>
          println("Exception: GetZpMainData/getData -> "+e.getMessage)
          e.printStackTrace()
      } finally {
        mongoClient.close()
      }
    }
  }

  def processBatch(data: MongoCursor): String = {
    var last_id = ""
    val tObj = new Tokenize
    //var jsList: List[JsObject] = List[JsObject]()
    for(x <- data) {
      try{
        val json = Json.parse(x.toString);
        //println(json)
        last_id = (json \ "_id" ).as[String].trim
        //println("id" + last_id)
        val suppData = (json \ "value").as[JsValue]
        val tSupName = tObj.tokenizeName((suppData \ "supname").as[String])
        val tSupAddr = tObj.tokenizeName((suppData \ "supaddr").as[String])
        val tConName = tObj.tokenizeName((suppData \ "conname").as[String])
        val tConAddr = tObj.tokenizeName((suppData \ "conaddr").as[String])
        val tN1Name = tObj.tokenizeName((suppData \ "n1name").as[String])
        val tN1Addr = tObj.tokenizeName((suppData \ "n1addr").as[String])
        val tN2Name = tObj.tokenizeName((suppData \ "n2name").as[String])
        val tN2Addr = tObj.tokenizeName((suppData \ "n2addr").as[String])
        val obj = Tokenized(last_id, tSupName, tSupAddr, tConName, tConAddr, tN1Name, tN1Addr, tN2Name, tN2Addr)
        updateMongo(obj)

      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    last_id
  }

  def updateMongo(obj: Tokenized) = {
    println(obj)
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      val _mObj = MongoDBObject("supname" -> obj.supname, "supaddr" -> obj.supaddr,
        "conname" -> obj.supname, "conaddr" -> obj.supaddr,
        "n1name" -> obj.supname, "n1addr" -> obj.supaddr,
        "n2name" -> obj.supname, "n2addr" -> obj.supaddr)
      val mObj = $set ("tokenized" -> _mObj)
      val query = MongoDBObject("_id" -> obj.id)

      val a = collection.update(query, mObj)
      println(a)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      mongoClient.close()
    }
  }

  case class Tokenized(id: String, supname: List[String], conname: List[String], n1name: List[String], n2name: List[String],
                       supaddr: List[String], conaddr: List[String], n1addr: List[String], n2addr: List[String])


}
