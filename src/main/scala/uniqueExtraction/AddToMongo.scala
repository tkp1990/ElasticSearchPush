package uniqueExtraction

import akka.actor.{ActorSystem, Actor}
import akka.actor.Actor.Receive
import api.mongo.MongoConfig
import play.api.Logger
import uniqueExtraction.Constants._

/**
 * Created by kenneththomas on 4/18/16.
 */
class AddToMongo(system: ActorSystem) extends Actor{

  override def receive: Receive = {
    case obj: Insert =>
      insertIntoMongo(obj)
  }

  /**
   * Takes an insert obj as input and inserts it into the DB.
   * The UNIQUEDATA db has a constraint on the ''p_text'' field, and wll throw an exception if a duplicate field is added.
   *
   * @param obj - data to be inserted into MongoDB
   */
  def insertIntoMongo(obj: Insert) = {
    val mongoClient = MongoConfig.getMongoClient(LOCALHOST, MONGODB_PORT)
    try {
      val collection = MongoConfig.getCollection(UNIQUE_DATA_DB_NAME, obj.process, mongoClient)
      Logger.debug("Adding data to MongoDB")
      for(x <- obj.jsonList) {
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
