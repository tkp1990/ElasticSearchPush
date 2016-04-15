
package tokenize

import java.net.InetAddress

import api.mongo.MongoConfig
import com.mongodb.casbah.Imports
import com.mongodb.casbah.Imports._
import dataCleaning.DataMaps
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.node.NodeBuilder
import play.api.libs.json._

/**
 * Created by kenneththomas on 4/10/16.
 */
class GetZPMainData {

  val INDEX = "tokenized"
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
    populateLocationMaps()
    val finalCount = 52982819
    var skip, c = 0
    val limit = 100
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
    var objList: List[Token] = List[Token]()
    for(x <- data) {
      try{
        val json = Json.parse(x.toString);
        last_id = (json \ "_id" ).as[String].trim
        val suppData = (json \ "value").as[JsValue]
        val tSupName = (suppData \ "supname").as[String]
        val tSupAddr = (suppData \ "supaddr").as[String]
        val tConName = (suppData \ "conname").as[String]
        val tConAddr = (suppData \ "conaddr").as[String]
        val tN1Name = (suppData \ "n1name").as[String]
        val tN1Addr = (suppData \ "n1addr").as[String]
        val tN2Name = (suppData \ "n2name").as[String]
        val tN2Addr = (suppData \ "n2addr").as[String]
        val obj = Token(last_id, tSupName, tSupAddr, tConName, tConAddr, tN1Name, tN1Addr, tN2Name, tN2Addr)
        objList = obj :: objList
        val resultList = tObj.tokenizeList(objList)
        val client = getClient(INDEX)
        try{
          if(resultList.length > 0){
            println("inserting into ES")
            bulkInsert(resultList, client)
          } else {
            println("Search Results is empty, Noting to insert")
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          client.close()
        }
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {

      }
    }
    last_id
  }

  /**
   *
   * @param index - ES Index
   * @return - ElasticSearch Client
   *
   */
  def getClient(index: String): Client = {
    val settings = Settings.settingsBuilder()
      .put("path.home", "/usr/share/elasticsearch")
      .put("cluster.name", "elasticsearch")
      .put("action.bulk.compress", true)
      .build();
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

  def bulkInsert(jsonList: List[XContentBuilder], client: Client): Unit ={
    val bulkRequest = client.prepareBulk();
    for (x <- jsonList) {
      println(x.prettyPrint().string())
      bulkRequest.add(client.prepareIndex(INDEX, "data")
        .setSource(x)
      );
    }
    val bulkResponse = bulkRequest.execute().actionGet();
    if (bulkResponse.hasFailures()) {
      // process failures by iterating through each bulk response item
    }
  }


  def populateLocationMaps() = {
    val mapDataUrl = SourceDir("./src/main/resources/CountryWithCodes.csv")
    val citiesDataUrl = SourceDir("./src/main/resources/cities_country.csv")
    val stateDataUrl = SourceDir("./src/main/resources/cities_region.csv")
    DataMaps.populateCountryMap(mapDataUrl)
    DataMaps.populateCitiesCountryMap(citiesDataUrl)
    DataMaps.populateStateCountryMap(stateDataUrl)
    DataMaps.populateCityMap(citiesDataUrl)
    DataMaps.populateStateMap(stateDataUrl)
  }

}

case class Token(id: String, supname: String, supaddr: String, conname: String, conaddr: String,
                 n1name: String, n1addr: String, n2name: String, n2addr: String)

case class SourceDir(path: String)