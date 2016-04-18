package uniqueExtraction

import akka.actor.{Props, ActorSystem, Actor}
import akka.actor.Actor.Receive
import api.mongo.MongoConfig
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoCollection
import play.api.Logger
import play.api.libs.json.{JsValue, Json}
import uniqueExtraction.Constants._
import com.mongodb.casbah.commons.MongoDBObject


/**
 * Created by kenneththomas on 4/18/16.
 */
class ExtractionSupervisor(system: ActorSystem) extends Actor {

  var count = 0

  override def receive: Receive = {
    case obj: Start =>
      Logger.debug("Application Start")
      getData("", obj.skip)
    case lastId: NextBatchRequest =>
      if(count > 0)
        count -= 1
      Logger.debug("Getting next set of Documents starting from ID: "+lastId.lastId)
      getData(lastId.lastId, 0)
  }

  /**
   *
   * Get data from mongo Collection and forwards to be preprocessed
   *
   * @param lastId - the Id of the last document in the previous collection.
   * @return
   */
  def getData(lastId: String, skip: Integer) = {
    val finalCount = MongoConfig.getCount(DB_NAME, COLLECTION_NAME)
    var recCount = skip
    var lastId = ""
    Logger.debug("Get Data Extraction  Supervisor")
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection(DB_NAME, COLLECTION_NAME, mongoClient)
      while(recCount < finalCount) {
        println("Count: " + recCount)
        var dataList: List[ZPMainObj] = List.empty[ZPMainObj]
        if(lastId.isEmpty || lastId.equals("")){
          val data = collection.find().skip(skip).limit(LIMIT).sort(Constants.orderBy)

          for(x <- data) {
            val json = Json.parse(x.toString);
            lastId = (json \ "_id" ).as[String].trim
            val suppData = (json \ "value").as[JsValue]
            val supName = (suppData \ "supname").as[String].toLowerCase()
            val conName = (suppData \ "conname").as[String].toLowerCase()
            val n1Name = (suppData \ "n1name").as[String].toLowerCase()
            val n2Name = (suppData \ "n2name").as[String].toLowerCase()
            val obj = ZPMainObj(lastId, supName, conName, n1Name, n2Name)
            dataList = obj :: dataList
            count = count + LIMIT
          }
        } else{
          val q = "_id" $gt (lastId)
          val data = collection.find(q).limit(LIMIT)

          for(x <- data) {
            val json = Json.parse(x.toString);
            lastId = (json \ "_id" ).as[String].trim
            val suppData = (json \ "value").as[JsValue]
            val supName = (suppData \ "supname").as[String].toLowerCase()
            val conName = (suppData \ "conname").as[String].toLowerCase()
            val n1Name = (suppData \ "n1name").as[String].toLowerCase()
            val n2Name = (suppData \ "n2name").as[String].toLowerCase()
            val obj = ZPMainObj(lastId, supName, conName, n1Name, n2Name)
            dataList = obj :: dataList
            count = count + LIMIT
          }
        }
        while(count >= 5){
          Thread.sleep(2000)
          Logger.debug("Processing waiting for Actor count to reduce!")
        }

        //Send data to be preprocessed
        val preProcessSupplierActor = system.actorOf(Props(new PreProcess(system)))
        preProcessSupplierActor ! CreateJsonList(dataList, SUPPLIER)

        val preProcessConsigneeActor = system.actorOf(Props(new PreProcess(system)))
        preProcessConsigneeActor ! CreateJsonList(dataList, CONSIGNEE)

        count += 1

        Logger.debug("Last Id being sent back to Supervisor: " + lastId)
        val supervisorActor = system.actorOf(Props(new ExtractionSupervisor(system)))
        supervisorActor ! NextBatchRequest(lastId)
      }

    } catch {
      case e: Exception =>
        println("Exception "+ e.getMessage)
        e.printStackTrace()
    } finally {
      mongoClient.close()
    }
  }


  /**
   * Check if Index exists, if does not exists creates it.
   *
   * @param collection
   */
  def createIndex(collection: MongoCollection) = {
    try{
      val index = collection.getIndexInfo.exists(x => x.getAs[String]("name").getOrElse("") == NAME_UNIQUE_INDEX)
      if(index){
        println("Index already Exists")
      } else {
        collection.createIndex(MongoDBObject("p_text"), NAME_UNIQUE_INDEX, true)
      }
    } catch{
      case e: Exception =>
        Logger.error("Exception while creating Index: "+e.getMessage)
        e.printStackTrace()
    } finally {

    }
  }

  /**
   *
   * @param db - database name
   * @param _collection - collection name
   * @return - number of documents in Collection
   */
  def getCount(db: String, _collection: String): Int = {
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try {
      val collection = MongoConfig.getCollection(db, _collection, mongoClient)
      collection.count()
    } catch {
      case e: Exception =>
        println("Exception GetCorporateAffiliatesData/getData: " + e.getMessage )
        e.printStackTrace()
        0
    } finally {
      mongoClient.close()
    }
  }

  /**
   * Setup the Databse and collections to which the Unique Data has to be added
   *
   */
  def setUpDBAndCollection() = {
    val mongoClient = MongoConfig.getMongoClient(LOCALHOST, MONGODB_PORT)
    try {
      val supCollection = MongoConfig.getCollection(UNIQUE_DATA_DB_NAME, SUPPLIER, mongoClient)
      createIndex(supCollection)
      val conCollection = MongoConfig.getCollection(UNIQUE_DATA_DB_NAME, CONSIGNEE, mongoClient)
      createIndex(conCollection)
    } catch {
      case e: Exception => Logger.error("Exception while trying to setup DB "+ e.getMessage)
        e.printStackTrace()
    } finally {
      mongoClient.close()
    }
  }
}
