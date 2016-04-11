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


  /**
   * GetData is the method which is called for the main execution
   * basic purpose is to get data in batches and then call the ProcessBatch method
   *
   */
  def getData () = {
    val finalCount = 52982819
    var skip, c = 2240000
    val limit = 1000
    var lastId = ""

    while (finalCount >= skip ) {
      println(" Count: "+skip)
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      try {
        val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
        if(skip == c) {
          val data = collection.find().skip(skip).limit(limit).sort(orderBy)
          skip = skip + limit
          lastId = processBatch(data)
          println("Last Id "+lastId)
        } else {
          val q = "_id" $gt (lastId)
          val data = collection.find(q).limit(limit)
          skip = skip + limit
          lastId = processBatch(data)
          println("Last Id: "+lastId)
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

  /**
   * Takes a Cursor as input and generates a List of Tokenized objects which have to be updated to their corresponding Id's
   * Also Returns a string which is the Id of the last entry in the Cursor (which is sorted)
   *
   * @param data
   * @return - Returns the last_id of the reterieved set of data from the Cursor
   */
  def processBatch(data: MongoCursor): String = {
    var last_id = ""
    val tObj = new Tokenize
    //var jsList: List[JsObject] = List[JsObject]()
    var objList: List[Tokenized] = List[Tokenized]()
    for(x <- data) {
      try{
        val json = Json.parse(x.toString);
        last_id = (json \ "_id" ).as[String].trim
        val suppData = (json \ "value").as[JsValue]
        val tSupName = tObj.tokenizeName((suppData \ "supname").as[String])
        val tSupAddr = tObj.tokenizeAddr((suppData \ "supaddr").as[String])
        val tConName = tObj.tokenizeName((suppData \ "conname").as[String])
        val tConAddr = tObj.tokenizeAddr((suppData \ "conaddr").as[String])
        val tN1Name = tObj.tokenizeName((suppData \ "n1name").as[String])
        val tN1Addr = tObj.tokenizeAddr((suppData \ "n1addr").as[String])
        val tN2Name = tObj.tokenizeName((suppData \ "n2name").as[String])
        val tN2Addr = tObj.tokenizeAddr((suppData \ "n2addr").as[String])
        val obj = Tokenized(last_id, tSupName, tSupAddr, tConName, tConAddr, tN1Name, tN1Addr, tN2Name, tN2Addr)
        objList = obj :: objList
        //updateMongo(obj)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
      }
    }
    bulkUpdate(objList)
    last_id
  }


  /**
   * Takes a Toenized object as input and performs the update
   *
   * @param obj - tokenized object
   */
  def updateMongo(obj: Tokenized) = {
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      val _mObj = MongoDBObject("supname" -> obj.supname, "supaddr" -> obj.supaddr,
        "conname" -> obj.conname, "conaddr" -> obj.conaddr,
        "n1name" -> obj.n1name, "n1addr" -> obj.n1addr,
        "n2name" -> obj.n2name, "n2addr" -> obj.n2addr)
      val mObj = $set ("tokenized" -> _mObj)
      val query = MongoDBObject("_id" -> obj.id)

      val a = collection.update(query, mObj)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      mongoClient.close()
    }
  }


  /**
   * Takes a list of Tokenized objects as input and preforms the update to the DB sequentially.
   *
   * @param objList - List of Tokenized object, which have to be added to the DataBase(updated to the DB actually)
   */
  def updateMongo(objList: List[Tokenized]) = {
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      for(obj <- objList) {
        val _mObj = MongoDBObject("supname" -> obj.supname, "supaddr" -> obj.supaddr,
          "conname" -> obj.conname, "conaddr" -> obj.conaddr,
          "n1name" -> obj.n1name, "n1addr" -> obj.n1addr,
          "n2name" -> obj.n2name, "n2addr" -> obj.n2addr)
        val mObj = $set ("tokenized" -> _mObj)
        val query = MongoDBObject("_id" -> obj.id)

        collection.update(query, mObj)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      println("UpdateMongo Sequentially done!!")
      mongoClient.close()
    }
  }


  /**
   * Takes a list of Tokenized objects and creates a UnorderedBulkoperation toperform
   * updates in bulk. If there is any exception the list is sent to updateMongo method
   * which takes a List as input and performs the update sequentially.
   *
   * @param objList - List of Tokenized object, which have to be added to the DataBase(updated to the DB actually)
   */
  def bulkUpdate(objList: List[Tokenized]) = {
    println("Calling bulk!!")
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      val bulkOp = collection.initializeUnorderedBulkOperation
      for( obj <- objList) {
        val _mObj = MongoDBObject("supname" -> obj.supname, "supaddr" -> obj.supaddr,
          "conname" -> obj.conname, "conaddr" -> obj.conaddr,
          "n1name" -> obj.n1name, "n1addr" -> obj.n1addr,
          "n2name" -> obj.n2name, "n2addr" -> obj.n2addr)
        val mObj = $set ("tokenized" -> _mObj)
        val query = MongoDBObject("_id" -> obj.id)
        bulkOp.find(query).update(mObj)
      }
      val result = bulkOp.execute()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("Exception!! calling update Mongo!!")
        updateMongo(objList)
    } finally {
      println("Bulk Done!!")
      mongoClient.close()
    }
  }

  case class Tokenized(id: String, supname: List[String], supaddr: List[String], conname: List[String], conaddr: List[String],
                       n1name: List[String], n1addr: List[String], n2name: List[String], n2addr: List[String])

}