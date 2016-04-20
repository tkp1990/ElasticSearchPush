package uniqueExtraction

import akka.actor.{Props, ActorSystem, Actor}
import akka.actor.Actor.Receive
import com.mongodb.casbah.MongoCursor
import dataCleaning.CleanData
import play.api.Logger
import uniqueExtraction.Constants._
import play.api.libs.json.{JsObject, JsValue, Json}

/**
 * Created by kenneththomas on 4/18/16.
 */
class PreProcess(system: ActorSystem) extends Actor{

  override def receive: Receive = {
    case data: CreateJsonList =>
      Logger.debug("Preprocessing Data")
      preProcessData(data.data, data.process, data.lastId)
  }


  /**
   * Loops through the List of ZPMain Objects sneds them for cleaning and then creates a List of Json objects to be inserted into MongoDB
   *
   * @param dataList - List of ZPMain Onjects
   */
  def preProcessData(dataList: List[ZPMainObj], process: String, lastId: String) = {
    var jsonList: List[JsObject] = List.empty[JsObject]
    println("List Length: "+dataList.length)
    for(x <- dataList) {
      val obj = checkForuniqueWithoutCleaning(x, process)
      jsonList = obj :: jsonList
    }

    val mongoAddActor = system.actorOf(Props(new AddToMongo(system)))
    mongoAddActor ! Insert(jsonList, process)
  }


  /**
   * A test method used to see how many unique records are available without cleaning the data
   *
   * @param obj - ZPMainObj, contains the data extracted from the Document
   * @param process - process from supplier or consignee
   */
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


  /**
   *
   * Takes a ZPMain object and cleans the string and returns a JsonObject
   *
   * @param obj - ZPMainObj, contains the data extracted from the Document
   * @param process - process from supplier or consignee
   * @return - returns a Json Object
   */
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

}
