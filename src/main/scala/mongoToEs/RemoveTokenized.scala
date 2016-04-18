package mongoToEs

import api.mongo.MongoConfig
import com.mongodb.casbah.Imports._

/**
 * Created by kenneththomas on 4/12/16.
 */
class RemoveTokenized {

  val ID = "_id"
  val orderBy = MongoDBObject(ID -> 1)
  val INDEX = "tokenized"
  val DB = "datacleaning"
  val COLLECTION = "ZPmainCollection"

  def remove() = {
    val finalCount = 300000
    var skip, c = 0
    val limit = 1000
    var last_id = ""
    while (finalCount >= skip) {
      val mongoClient = MongoConfig.getMongoClient("localhost", 27017)
      val collection = MongoConfig.getCollection(DB, COLLECTION, mongoClient)
      if(skip == c) {
        val data = collection.find().skip(skip).limit(limit).sort(orderBy)
        skip = skip + limit
      } else {

      }
    }
  }
}
