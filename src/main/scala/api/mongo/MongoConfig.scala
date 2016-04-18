package api.mongo

import com.mongodb.casbah.Imports._
import play.api.libs.json.JsObject

/**
 * Created by kenneththomas on 4/6/16.
 */
object MongoConfig {


  /**
   *
   * Returns a MongoDb Collection for a specified database
   *
   * @param database - database name where the Collection is located
   * @param collectionName - collection name
   * @param mongoClient - mongoDb client
   * @return - returns collection of type MongoCollection
   */
  def getCollection(database: String, collectionName: String, mongoClient: MongoClient): MongoCollection = {
    val db = mongoClient(database)
    val collection = db(collectionName)
    collection
  }

  /**
   * Returns a MongoClient object
   *
   * @param host - mongo hostname
   * @param port - the port mongoDb is running on
   * @return - MongoClient
   */
  def getMongoClient(host: String, port: Int): MongoClient = {
    MongoClient(host, port)
  }

  /**
   * Takes a Toenized object as input and performs the update
   *
   * @param obj - tokenized object
   */
  def updateMongo(obj: JsObject) = {
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      val id = (obj \ "id").as[String]
      val _mObj = MongoDBObject(obj.toString())
      val mObj = $set ("tokenized" -> _mObj)
      val query = MongoDBObject("_id" -> id)

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
  def updateMongo(objList: List[JsObject]) = {
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection("datacleaning", "ZPmainCollection", mongoClient)
      for(obj <- objList) {
        val id = (obj \ "id").as[String]
        val _mObj = MongoDBObject(obj.toString())
        val mObj = $set ("tokenized" -> _mObj)
        val query = MongoDBObject("_id" -> id)

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
  def bulkUpdate(objList: List[JsObject], db: String, collectionName: String) = {
    println("Calling bulk!!")
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection(db, collectionName, mongoClient)
      val bulkOp = collection.initializeUnorderedBulkOperation
      for( obj <- objList) {
        val id = (obj \ "id").as[String]
        val _mObj = MongoDBObject(obj.toString())
        val mObj = $set ("tokenized" -> _mObj)
        val query = MongoDBObject("_id" -> id)
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

  /**
   *
   * @param db - name of the database
   * @param collectionName - collection name
   * @return - count in the particular collection
   */
  def getCount(db: String, collectionName: String): Integer = {
    var count = 0
    val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
    try{
      val collection = MongoConfig.getCollection(db, collectionName, mongoClient)
      count = collection.count()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      mongoClient.close()
    }
    count
  }

  /**
   * Insert data into MongoDB Collection
   *
   * @param obj - Document to be added to a collection
   * @param collection - Collection to which data has to be added
   * @return - Write Result
   */
  def insert(obj: JsObject, collection: MongoCollection): WriteResult = {
    val res = collection.insert(MongoDBObject(obj.toString()))
    res
  }
}
