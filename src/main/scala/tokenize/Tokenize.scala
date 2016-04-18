package tokenize

import dataCleaning.CleanData
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import play.api.libs.json.{JsString, JsObject, Json}

import scala.util.matching.Regex

/**
 * Created by kenneththomas on 4/10/16.
 */
class Tokenize {

  val nameRegex1 = """[-,\,,.,!,',/]""".r
  val nameRegex2 = """[,,.,']""".r
  val stopTexts = """[.,]""".r
  val addrRegex1 = """[-,#.,,,+,!,',(,),/,\,&,%,$,*,@,:,`,?]""".r
  val VARIANCE = "variance"
  val PARENT = "parent"
  val INDEX = "tokenized"

  def tokenizeAndInsert(objList: List[Token]) = {
    val obj = new GetZPMainData
    val list = tokenizeList(objList)
    val client = obj.getClient(INDEX)
    try{
      if(list.length > 0){
        println("inserting into ES")
        obj.bulkInsert(list, client)
      } else {
        println("Search Results is empty, Noting to insert")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      client.close()
    }
  }

  def tokenizeList(objList: List[Token]) = {
    val gZPObj = new GetZPMainData
    gZPObj.populateLocationMaps()
    var jsonList: List[XContentBuilder] = List.empty[XContentBuilder]
    for( obj <- objList ) {
      val rawSupplierData = obj.supname + " " +obj.supaddr
      val rawConsigneeData = obj.conname + " " +obj.conaddr
      val rawN1Data = obj.n1name + " " +obj.n1addr
      val rawN2Data = obj.n2name + " " +obj.n2addr
      //var jsonObj = Json.obj()
      var jsonObj = jsonBuilder().startObject()
      //adding Supplier Data
      jsonObj.startObject("supname")
        .field("raw",obj.supname)
        .field("t1", tokenizeName(obj.supname, jsonObj))
        .endObject()
      jsonObj.startObject("supaddr")
        .field("raw",obj.supaddr)
        .field("t1", tokenizeName(obj.supaddr, jsonObj))
        .endObject()
      jsonObj.startObject("supplier")
        .field("raw",rawSupplierData)
        .field("p1", processData(rawSupplierData, jsonObj))
        .endObject()
      //adding consignee data
      jsonObj.startObject("conname")
        .field("raw",obj.conname)
        .field("t1", tokenizeName(obj.conname, jsonObj))
        .endObject()
      jsonObj.startObject("conaddr")
        .field("raw",obj.conaddr)
        .field("t1", tokenizeName(obj.conaddr, jsonObj))
        .endObject()
      jsonObj.startObject("consignee")
        .field("raw",rawConsigneeData)
        .field("p1", processData(rawConsigneeData, jsonObj))
        .endObject()
      /*jsonObj.field("conname", jsonObj.startObject().field("raw",obj.conname).field("tokenized", tokenizeName(obj.conname, jsonObj)).endObject())
      jsonObj.field("conaddr", jsonObj.startObject().field("raw",obj.conaddr).field("tokenized", tokenizeAddr(obj.conaddr, jsonObj)).endObject())
      jsonObj.field("consignee", jsonObj.startObject().field("raw",rawConsigneeData).field("processed", processData(rawConsigneeData, jsonObj)).endObject())*/
      //adding n1 data
      jsonObj.startObject("n1name")
        .field("raw",obj.n1name)
        .field("t1", tokenizeName(obj.n1name, jsonObj))
        .endObject()
      jsonObj.startObject("n1addr")
        .field("raw",obj.n1addr)
        .field("t1", tokenizeName(obj.n1addr, jsonObj))
        .endObject()
      jsonObj.startObject("n1data")
        .field("raw",rawN1Data)
        .field("p1", processData(rawN1Data, jsonObj))
        .endObject()
      /*jsonObj.field("n1name", jsonObj.startObject().field("raw",obj.n1name).field("tokenized", tokenizeName(obj.n1name, jsonObj)).endObject())
      jsonObj.field("n1addr", jsonObj.startObject().field("raw",obj.n1addr).field("tokenized", tokenizeName(obj.n1addr, jsonObj)).endObject())
      jsonObj.field("n1data", jsonObj.startObject().field("raw",rawN1Data).field("processed", processData(rawN1Data, jsonObj)).endObject())*/
      //adding n2 data
      jsonObj.startObject("n2name")
        .field("raw",obj.n2name)
        .field("t1", tokenizeName(obj.n2name, jsonObj))
        .endObject()
      jsonObj.startObject("n2addr")
        .field("raw",obj.n2addr)
        .field("t1", tokenizeName(obj.n2addr, jsonObj))
        .endObject()
      jsonObj.startObject("n2data")
        .field("raw",rawN2Data)
        .field("p1", processData(rawN2Data, jsonObj))
        .endObject()
      /*jsonObj.field("n2name", jsonObj.startObject().field("raw",obj.n2name).field("tokenized", tokenizeName(obj.n2name, jsonObj)).endObject())
      jsonObj.field("n2addr", jsonObj.startObject().field("raw",obj.n2addr).field("tokenized", tokenizeName(obj.n2addr, jsonObj)).endObject())
      jsonObj.field("n2data", jsonObj.startObject().field("raw",rawN2Data).field("processed", processData(rawN2Data, jsonObj)).endObject())*/
      //adding json object to json list
      jsonObj.endObject()
      jsonList = jsonObj :: jsonList
      //println(Json.prettyPrint(jsonObj))
    }
    jsonList
  }

  def tokenizeObj(obj: Token): XContentBuilder = {
    val rawSupplierData = obj.supname + " " +obj.supaddr
    val rawConsigneeData = obj.conname + " " +obj.conaddr
    val rawN1Data = obj.n1name + " " +obj.n1addr
    val rawN2Data = obj.n2name + " " +obj.n2addr
    //var jsonObj = Json.obj()
    var jsonObj = jsonBuilder().startObject()

    //adding Supplier Data
    jsonObj.startObject("supname")
      .field("raw",obj.supname)
      .field("t1", tokenizeName(obj.supname, jsonObj))
      .endObject()
    jsonObj.startObject("supaddr")
      .field("raw",obj.supaddr)
      .field("t1", tokenizeName(obj.supaddr, jsonObj))
      .endObject()
    jsonObj.startObject("supplier")
      .field("raw",rawSupplierData)
      .field("p1", processData(rawSupplierData, jsonObj))
      .endObject()
    //adding consignee data
    jsonObj.startObject("conname")
      .field("raw",obj.conname)
      .field("t1", tokenizeName(obj.conname, jsonObj))
      .endObject()
    jsonObj.startObject("conaddr")
      .field("raw",obj.conaddr)
      .field("t1", tokenizeName(obj.conaddr, jsonObj))
      .endObject()
    jsonObj.startObject("consignee")
      .field("raw",rawConsigneeData)
      .field("p1", processData(rawConsigneeData, jsonObj))
      .endObject()
    //adding n1 data
    jsonObj.startObject("n1name")
      .field("raw",obj.n1name)
      .field("t1", tokenizeName(obj.n1name, jsonObj))
      .endObject()
    jsonObj.startObject("n1addr")
      .field("raw",obj.n1addr)
      .field("t1", tokenizeName(obj.n1addr, jsonObj))
      .endObject()
    jsonObj.startObject("n1data")
      .field("raw",rawN1Data)
      .field("p1", processData(rawN1Data, jsonObj))
      .endObject()
    //adding n2 data
    jsonObj.startObject("n2name")
      .field("raw",obj.n2name)
      .field("t1", tokenizeName(obj.n2name, jsonObj))
      .endObject()
    jsonObj.startObject("n2addr")
      .field("raw",obj.n2addr)
      .field("t1", tokenizeName(obj.n2addr, jsonObj))
      .endObject()
    jsonObj.startObject("n2data")
      .field("raw",rawN2Data)
      .field("p1", processData(rawN2Data, jsonObj))
      .endObject()
    jsonObj.endObject()
    jsonObj
  }

  def tokenizeString(str: String, regex: Regex) = {
    val tokenz = str.split(" ")
    var tokenMap: Map[String, Set[String]] = Map()
    for( a <- tokenz ){
      tokenMap = tokenMap + (a -> Set.empty[String])
    }
    val keySet = tokenMap.keySet.toIterator
    while(keySet.hasNext) {
      var tempList: Set[String] = Set[String]()
      val t = keySet.next()
      tempList = tempList ++ recursiveSubstitute(t, regex)
      tokenMap = tokenMap + (t -> tempList)
    }
    //tokenMap.foreach(x => println(x._1 + ": " + x._2))
    getJson(tokenMap, str)
  }

  def tokenizeName(name: String, jsonObj: XContentBuilder) = {
    val tokenz = name.split(" ")
    var tokenMap: Map[String, Set[String]] = Map()
    for( a <- tokenz ){
      tokenMap = tokenMap + (a -> Set.empty[String])
    }
    val keySet = tokenMap.keySet.toIterator
    while(keySet.hasNext) {
      var tempList: Set[String] = Set[String]()
      val t = keySet.next()
      if(!t.equals("-1")){
        tempList = tempList ++ recursiveSubstitute(t, nameRegex1)
        tokenMap = tokenMap + (t -> tempList)
      }
    }
    //tokenMap.foreach(x => println(x._1 + ": " + x._2))
    //val jsonObj = getJson(tokenMap, name)
    val newObj = getConObj(tokenMap, name, jsonObj)
  }

  def tokenizeAddr(addr: String, jsonObj: XContentBuilder) = {
    val tokenz = addr.split(" ")
    var tokenMap: Map[String, Set[String]] = Map()
    for( a <- tokenz ){
      tokenMap = tokenMap + (a -> Set.empty[String])
    }
    val keySet = tokenMap.keySet.toIterator
    while(keySet.hasNext) {
      var tempList: Set[String] = Set[String]()
      val t = keySet.next()
      if(!t.equals("-1")){
        tempList = tempList ++ recursiveSubstitute(t, addrRegex1)
        tokenMap = tokenMap + (t -> tempList)
      }
    }
    //tokenMap.foreach(x => println(x._1 + ": " + x._2))
    //val jsonObj = getJson(tokenMap, addr)
    val newObj = getConObj(tokenMap, addr, jsonObj)
  }

  def recursiveSubstitute(str: String, regex: Regex): Set[String] = {
    var subList: Set[String] = Set[String]()
    val matches = regex.findAllIn(str)
    while(matches.hasNext){
      val a = matches.next()
      val spaceSubstitute = str.replace(a, " ")
      subList += spaceSubstitute
      var subMatches = regex.findAllIn(spaceSubstitute)
      if(subMatches.hasNext){
        val tempList: Set[String] = recursiveSubstitute(spaceSubstitute, regex)
        subList = subList ++ tempList
      }
      val emptySubstitute = str.replace(a, "")
      subList += emptySubstitute
      subMatches = regex.findAllIn(emptySubstitute)
      if(subMatches.hasNext){
        val tempList: Set[String] = recursiveSubstitute(spaceSubstitute, regex)
        subList = subList ++ tempList
      }
    }
    subList
  }

  def getConObj(map: Map[String, Set[String]], parent: String, jsonObj: XContentBuilder): XContentBuilder = {
    jsonObj.startObject("tokenized")
    for ( x <- map) {
      val a = x._2.toList
      if(a.size > 0){
        jsonObj.startObject(x._1.replace("."," ").replace(","," "))
        jsonObj.startObject(VARIANCE)
        for( y <- 0 to a.size - 1) {
          jsonObj.field("v"+y, a(y))
        }
        jsonObj.endObject()
        jsonObj.field(PARENT, x._1)
        jsonObj.endObject()
      }
    }
    jsonObj.endObject()
    //jsonObj
  }

  def getJson(map: Map[String, Set[String]], parent: String): JsObject = {
    var jsonList: List[JsObject] = List.empty[JsObject]
    var ret = Json.obj()
    for( x <- map) {
      val a = x._2.toList
      var value = Json.obj()
      for( y <- 0 to a.size - 1) {
        value = value ++ Json.obj("v"+y -> a(y))
      }
      if(a.size > 0){
        val jObj = Json.obj(VARIANCE -> value, PARENT -> x._1)
        jsonList = jObj :: jsonList
        ret = ret ++ Json.obj(x._1 -> jObj)
      }
    }
    //println(ret)
    ret
  }

  def processData(rawData: String, jsonObj: XContentBuilder) = {
    val processedObj = CleanData.processData(rawData)
    jsonObj.startObject("processed")

    jsonObj.field("name", processedObj.name)
    jsonObj.field("country", processedObj.location.country)
    jsonObj.field("city", processedObj.location.city)
    jsonObj.field("state", processedObj.location.state)
    jsonObj.field("street", processedObj.street)
    jsonObj.field("pincode", processedObj.location.pinCode)

    jsonObj.endObject()
    /*var obj = Json.obj("name" -> processedObj.name, "country" -> processedObj.location.country, "city" -> processedObj.location.city,
              "state" -> processedObj.location.state, "street" -> processedObj.street, "pincode" -> processedObj.location.pinCode)
    obj*/

    //jsonObj
  }


}

case class NameData()

case class AddrData()

/**

jsonObj = jsonObj ++ Json.obj("supname" -> Json.obj("raw" -> obj.supname, "tokenized" -> tokenizeName(obj.supname)))
      jsonObj = jsonObj ++ Json.obj("supaddr" -> Json.obj("raw" -> obj.supaddr, "tokenized" -> tokenizeAddr(obj.supaddr)))
      jsonObj = jsonObj ++ Json.obj("supplier" -> Json.obj("raw" -> rawSupplierData, "processed" -> processData(rawSupplierData)))
      //adding consignee data
      jsonObj = jsonObj ++ Json.obj("conname" -> Json.obj("raw" -> obj.conname, "tokenized" -> tokenizeName(obj.conname)))
      jsonObj = jsonObj ++ Json.obj("conaddr" -> Json.obj("raw" -> obj.conaddr, "tokenized" -> tokenizeAddr(obj.conaddr)))
      jsonObj = jsonObj ++ Json.obj("consignee" -> Json.obj("raw" -> rawConsigneeData, "processed" -> processData(rawConsigneeData)))
      //adding n1 data
      jsonObj = jsonObj ++ Json.obj("n1name" -> Json.obj("raw" -> obj.n1name, "tokenized" -> tokenizeName(obj.n1name)))
      jsonObj = jsonObj ++ Json.obj("n1addr" -> Json.obj("raw" -> obj.n1addr, "tokenized" -> tokenizeAddr(obj.n1addr)))
      jsonObj = jsonObj ++ Json.obj("n1data" -> Json.obj("raw" -> rawN1Data, "processed" -> processData(rawN1Data)))
      //adding n2 data
      jsonObj = jsonObj ++ Json.obj("n2name" -> Json.obj("raw" -> obj.n2name, "tokenized" -> tokenizeName(obj.n2name)))
      jsonObj = jsonObj ++ Json.obj("n2addr" -> Json.obj("raw" -> obj.n2addr, "tokenized" -> tokenizeAddr(obj.n2addr)))
      jsonObj = jsonObj ++ Json.obj("n2data" -> Json.obj("raw" -> rawN2Data, "processed" -> processData(rawN2Data)))

  */