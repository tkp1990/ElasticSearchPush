package api.mongo

import com.mongodb.casbah.Imports._

/**
 * Created by kenneththomas on 4/6/16.
 */
class MongoConfig {


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
}
