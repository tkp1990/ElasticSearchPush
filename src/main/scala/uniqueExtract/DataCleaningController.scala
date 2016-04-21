package uniqueExtract

import api.mongo.MongoConfig
import dataCleaning.CleanData
import play.api.Logger
import play.api.libs.json.{JsObject, JsValue, Json}
import uniqueExtraction.Constants._
import com.mongodb.casbah.Imports._
import uniqueExtraction.{Insert, ZPMainObj}

/**
 * Created by kenneththomas on 4/20/16.
 */
class DataCleaningController {

  def getData(_skip: Integer) = {
    val finalCount = MongoConfig.getCount(DB_NAME, COLLECTION_NAME)
    var skip, c = _skip
    val limit = 10000
    var lastId = ""

    while( finalCount > skip ) {
      val mongoClient = MongoConfig.getMongoClient(LOCALHOST, MONGODB_PORT)
      try {
        val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
        if(skip == c) {
          val data = collection.find().skip(skip).limit(limit).sort(orderBy)
          skip = skip + limit
          lastId = processCursor(data)
          println("Last Id "+lastId)
        } else {
          val q = "_id" $gt (lastId)
          val data = collection.find(q).limit(limit)
          skip = skip + limit
          lastId = processCursor(data)
          println("Last Id: "+lastId)
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        mongoClient.close()
      }
    }
  }

  def processCursor(data: MongoCursor): String = {
    var lastId = ""
    var objList: List[ZPMainObj] = List.empty[ZPMainObj]
    for(x <- data) {
      val json = Json.parse(x.toString);
      lastId = (json \ "_id" ).as[String].trim
      val suppData = (json \ "value").as[JsValue]
      val supName = (suppData \ "supname").as[String].toLowerCase()
      val conName = (suppData \ "conname").as[String].toLowerCase()
      val n1Name = (suppData \ "n1name").as[String].toLowerCase()
      val n2Name = (suppData \ "n2name").as[String].toLowerCase()
      val obj = ZPMainObj(lastId, supName, conName, n1Name, n2Name)
      objList = obj :: objList
    }
    getJsonList(objList, SUPPLIER)
    getJsonList(objList, CONSIGNEE)
    lastId
  }

  def getJsonList(objList: List[ZPMainObj], process: String) = {
    var jsonList: List[JsObject] = List.empty[JsObject]
    //println("List Length: "+objList.length)
    for(x <- objList) {
      val obj = cleanName(x, process)
      jsonList = obj :: jsonList
    }
    insertIntoMongo(jsonList, process)
  }

  def checkForuniqueWithoutCleaning(obj: ZPMainObj, process: String): JsObject = {
    process match {
      case SUPPLIER =>
        var extractedFrom = ""
        var _name = obj.supName
        extractedFrom = "supname"
        if(_name.isEmpty || _name.contains(LOGISTICS)){
          _name = obj.n1Name
          extractedFrom = "n1name"
        } else if (_name.isEmpty) {
          _name = obj.n2Name
          extractedFrom = "n2name"
        }
        Json.obj("mid" -> obj.id, "p_text" -> _name, "supname" -> obj.supName, "dataFrom" -> extractedFrom)

      case CONSIGNEE =>
        var extractedFrom = ""
        var _name = obj.conName
        extractedFrom = "conname"
        if(_name.isEmpty || _name.contains(LOGISTICS)){
          _name = obj.n1Name
          extractedFrom = "n1name"
        } else if (_name.isEmpty) {
          _name = obj.n2Name
          extractedFrom = "n2name"
        }
        Json.obj("mid" -> obj.id, "p_text" -> _name, "conname" -> obj.conName, "dataFrom" -> extractedFrom)
    }
  }

  def cleanName(obj: ZPMainObj, process: String): JsObject = {
    process match {
      case SUPPLIER =>
        var extractedFrom = ""
        var _name = obj.supName
        extractedFrom = "supname"
        if(_name.isEmpty || _name.contains(LOGISTICS)){
          _name = obj.n1Name
          extractedFrom = "n1name"
        } else if (_name.isEmpty) {
          _name = obj.n2Name
          extractedFrom = "n2name"
        }
        val bType = CleanData.checkIfStringHasBusinessType(_name)
        val fName = bType match {
          case Right(businessType) =>
            _name.replace(businessType, "")
          case Left(msg) =>
            _name
        }
        val f2Name = PRE_PROCESS_REGEX.replaceAllIn(fName, " ")
        val filteredName = f2Name.replace("  ", " ").trim
        Json.obj("mid" -> obj.id, "p_text" -> filteredName, "supname" -> obj.supName, "dataFrom" -> extractedFrom)

      case CONSIGNEE =>
        var extractedFrom = ""
        var _name = obj.conName
        extractedFrom = "conname"
        if(_name.isEmpty || _name.contains(LOGISTICS)){
          _name = obj.n1Name
          extractedFrom = "n1name"
        } else if (_name.isEmpty) {
          _name = obj.n2Name
          extractedFrom = "n2name"
        }
        val bType = CleanData.checkIfStringHasBusinessType(_name)
        val fName = bType match {
          case Right(businessType) =>
            _name.replace(businessType, "")
          case Left(msg) =>
            _name
        }
        val f2Name = PRE_PROCESS_REGEX.replaceAllIn(fName, " ")
        val filteredName = f2Name.replace("  ", " ").trim
        Json.obj("mid" -> obj.id, "p_text" -> filteredName, "conname" -> obj.conName, "dataFrom" -> extractedFrom)
    }
  }

  def insertIntoMongo(jsonList: List[JsObject], process: String) = {
    val mongoClient = MongoConfig.getMongoClient(LOCALHOST, MONGODB_PORT)
    try {
      val collection = MongoConfig.getCollection(UNIQUE_DATA_DB_NAME, process, mongoClient)
      Logger.debug("Adding data to MongoDB")
      for(x <- jsonList) {
        MongoConfig.insert(x, collection)
      }
    } catch {
      case dupE: com.mongodb.DuplicateKeyException =>
      //Do nothing this is expected
      //Logger.error("Duplicate Key Exception")
      case e: Exception =>
        Logger.error("Exception while adding data to MongoDB: "+e.getMessage)
        e.printStackTrace()
    } finally {
      mongoClient.close()
    }
  }
}
