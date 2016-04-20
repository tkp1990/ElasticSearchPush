package uniqueExtraction

import akka.actor.{Props, ActorSystem, Actor}
import akka.actor.Actor.Receive
import com.mongodb.casbah.MongoCursor
import play.api.Logger
import play.api.libs.json.{JsValue, Json}
import uniqueExtraction.Constants._

/**
 * Created by kenneththomas on 4/18/16.
 */
class ExtractForPreProcessing(system: ActorSystem) extends Actor{
  override def receive: Receive = {
    case data: PreProcessData =>
      getCollectionData(data.data)
  }

  /**
   *
   * @param data - Mongo Cursor with Collection data to be extracted and sent to be preprocessed
   */
  def getCollectionData(data: MongoCursor) = {
    var lastId = ""
    var dataList: List[ZPMainObj] = List.empty[ZPMainObj]
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
    }
    //Send data to be preprocessed
    val preProcessSupplierActor = system.actorOf(Props(new PreProcess(system)))
    preProcessSupplierActor ! CreateJsonList(dataList, SUPPLIER, lastId)

    val preProcessConsigneeActor = system.actorOf(Props(new PreProcess(system)))
    preProcessConsigneeActor ! CreateJsonList(dataList, CONSIGNEE, lastId)

    //Send lastId back to Supervisor
    Logger.debug("Last Id being sent back to Supervisor: " + lastId)
    val supervisorActor = system.actorOf(Props(new ExtractionSupervisor(system)))
    supervisorActor ! NextBatchRequest(lastId, supervisorActor)
  }
}
