package uniqueLocation

import api.mongo.MongoConfig
import com.mongodb.casbah.Imports
import com.mongodb.casbah.Imports._
import org.bson.types.ObjectId
import play.api.libs.json._
import uniqueExtraction.Constants._

/**
 * Created by kenneththomas on 4/19/16.
 */
class GetSupplierAddress {

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

  def getData() = {
    val finalCount = MongoConfig.getCount("uniqueData","supplier")
    var skip, c = 0
    val limit = 100
    var lastId = ""
    while (finalCount >= skip ) {
      println(" Count: "+skip)
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      try {
        val collection = MongoConfig.getCollection("uniqueData", "supplier", mongoClient)
        if(skip == c) {
          val data = collection.find().skip(skip).limit(limit).sort(orderBy)
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
          println("Exception: GetSupplierAddress/getData -> "+e.getMessage)
          e.printStackTrace()
      } finally {
        mongoClient.close()
      }
    }
  }

  def extractData(data: MongoCursor) = {
    var last_id = ""
    var objList: List[UniqSupplier] = List.empty[UniqSupplier]
    for(x <- data) {
      val json = Json.parse(x.toString);
      last_id = (json \ "_id" ).as[String].trim
      val supname = (json \ "supname").as[String].trim
      val mid = (json \ "mid").as[String].trim
      val pText = (json \ "p_text").as[String].trim
      val obj = UniqSupplier(mid, supname, pText)
      objList = obj :: objList

    }
  }


  def getAddressFromES(obj: UniqSupplier) = {

  }


  /**
   * GET ADDRESS FROM MONGO DATACLEANING
   * @param obj
   */
  def getAddress(obj: UniqSupplier) = {
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      val query = MongoDBObject("_id" -> obj.mid)
      val data = collection.find(query)
      for(x <- data) {
        val json = Json.parse(x.toString);
        val suppData = (json \ "value").as[JsValue]
        val tSupName = (suppData \ "supname").as[String]
        val tSupAddr = (suppData \ "supaddr").as[String]
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      mongoClient.close()
    }
  }
}

case class UniqSupplier(mid: String, supplier: String, p_text: String)

case class SupplierAddress(country: String, state: String, cit: String, pincode: String, street: String)