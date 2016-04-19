package uniqueExtraction

import com.mongodb.casbah.MongoCursor
import play.api.libs.json.JsObject

/**
 * Created by kenneththomas on 4/18/16.
 */
class Messages {

}

case class PreProcessData(data: MongoCursor)

case class Start(skip: Integer)

case class CreateJsonList(data: List[ZPMainObj], process: String, lastId: String)

case class Insert(jsonList: List[JsObject], process: String)

case class ZPMainObj(id: String, supName: String, conName: String, n1Name: String, n2Name: String)

case class NextBatchRequest(lastId: String)
