package mongoToEs

import java.net.InetAddress

import com.mongodb.casbah.Imports
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.ValidBSONType.ObjectId
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.node.NodeBuilder._
import org.elasticsearch.common.xcontent.XContentFactory._
import play.api.libs.json._


/**
 * Created by kenneththomas on 4/6/16.
 */
class EsJavaApi {

  val INDEX = "supplier2"
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
    val finalCount = 52982819
    var skip, c = 25562000
    val limit = 10000
    var last_id = ""
    while (finalCount >= skip ) {
      println(" Count: "+skip)
      val orderBy = MongoDBObject("_id" -> 1)
      val mongoClient = getMongoClient("localhost", 27017)
      val (collection, mdbClient) = getCollection("datacleaning", "ZPmainCollection", mongoClient)
      try {
        var jsonList: List[JsObject] = List[JsObject]()
        var jsList: List[JsValue] = List[JsValue]()
        if(skip == c){
          val data = collection.find().skip(skip).limit(limit).sort(orderBy)
          skip = skip + limit

          for(x <- data) {
            val json = Json.parse(x.toString);
            println(json)
            last_id = (json \ "_id" ).as[String].trim
            println("id" + last_id)
            val supplier = (json \ "value").as[JsValue]
            //val jObj = Json.obj("data" -> supplier)
            //println(json.toString())
            //jsonList = jObj :: jsonList
            jsList = supplier :: jsList
          }
          val client = getClient(INDEX)
          try{
            bulkInsert(jsList, client)
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            client.close()
          }
        } else {
          val q = "_id" $gt (last_id)
          val data = collection.find(q).limit(limit)
          skip = skip + limit

          for(x <- data) {
            val json = Json.parse(x.toString);
            last_id = (json \ "_id" ).as[String].trim
            val supplier = (json \ "value").as[JsValue]
            //val jObj = Json.obj("data" -> supplier)
            //println(json.toString())
            //jsonList = jObj :: jsonList
            jsList = supplier :: jsList
          }
          val client = getClient(INDEX)
          try{
            if(jsList.length > 0){
              bulkInsert(jsList, client)
            } else {
              println("Search Results is empty, Noting to insert")
            }
          } catch {
            case e: Exception => e.printStackTrace()
          } finally {
            client.close()
          }
        }
      } catch {
        case e: Exception => println("Exception: "+ e.getMessage)
      } finally {
        mdbClient.close()
      }
    }
  }

  def getClient(index: String): Client = {
    val settings = Settings.settingsBuilder()
      .put("path.home", "/usr/share/elasticsearch")
      .put("cluster.name", "elasticsearch")
      .put("action.bulk.compress", true)
      .build();
    //val node = nodeBuilder().local(true).settings(settings).node();
    val client = TransportClient.builder().settings(settings).build();
    client.addTransportAddresses(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300))
    /*val client = NodeBuilder.nodeBuilder()
      .client(true)
      .node()
      .client();*/
    val indexExists = client.admin().indices().prepareExists(index).execute().actionGet().isExists();
    if (!indexExists) {
      client.admin().indices().prepareCreate(index).execute().actionGet();
    }
    client
  }

  def bulkInsert(jsonList: List[JsValue], client: Client): Unit ={
    val bulkRequest = client.prepareBulk();
    for (x <- jsonList) {
      bulkRequest.add(client.prepareIndex(INDEX, "data")
        .setSource(jsonBuilder().startObject().field("data", x).endObject())
      );
    }
    val bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      // process failures by iterating through each bulk response item
    }
  }

  def getCollection(_db: String, _collection: String, mongoClient: MongoClient): (MongoCollection, MongoClient) = {
    val db = mongoClient(_db)
    val collection = db(_collection)
    (collection, mongoClient)
  }

  def getMongoClient(host: String, port: Int): MongoClient = {
    MongoClient(host, port)
  }

}
