package tokenize

import dataCleaning.CleanData
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


  def tokenizeList(objList: List[Token]) = {
    val gZPObj = new GetZPMainData
    gZPObj.populateLocationMaps()
    var jsonList: List[JsObject] = List.empty[JsObject]
    for( obj <- objList ) {
      val rawSupplierData = obj.supname + " " +obj.supaddr
      val rawConsigneeData = obj.conname + " " +obj.conaddr
      val rawN1Data = obj.n1name + " " +obj.n1addr
      val rawN2Data = obj.n2name + " " +obj.n2addr
      var jsonObj = Json.obj()
      //adding Supplier Data
      jsonObj = jsonObj ++ Json.obj("supname" -> Json.obj("rawData" -> obj.supname, "tokenized" -> tokenizeName(obj.supname)))
      jsonObj = jsonObj ++ Json.obj("supaddr" -> Json.obj("rawData" -> obj.supaddr, "tokenized" -> tokenizeAddr(obj.supaddr)))
      jsonObj = jsonObj ++ Json.obj("supplier" -> Json.obj("rawData" -> rawSupplierData, "processed" -> processData(rawSupplierData)))
      //adding consignee data
      jsonObj = jsonObj ++ Json.obj("conname" -> Json.obj("rawData" -> obj.conname, "tokenized" -> tokenizeName(obj.conname)))
      jsonObj = jsonObj ++ Json.obj("conaddr" -> Json.obj("rawData" -> obj.conaddr, "tokenized" -> tokenizeAddr(obj.conaddr)))
      jsonObj = jsonObj ++ Json.obj("consignee" -> Json.obj("rawData" -> rawConsigneeData, "processed" -> processData(rawConsigneeData)))
      //adding n1 data
      jsonObj = jsonObj ++ Json.obj("n1name" -> Json.obj("rawData" -> obj.n1name, "tokenized" -> tokenizeName(obj.n1name)))
      jsonObj = jsonObj ++ Json.obj("n1addr" -> Json.obj("rawData" -> obj.n1addr, "tokenized" -> tokenizeAddr(obj.n1addr)))
      jsonObj = jsonObj ++ Json.obj("n1data" -> Json.obj("rawData" -> rawN1Data, "processed" -> processData(rawN1Data)))
      //adding n2 data
      jsonObj = jsonObj ++ Json.obj("n2name" -> Json.obj("rawData" -> obj.n2name, "tokenized" -> tokenizeName(obj.n2name)))
      jsonObj = jsonObj ++ Json.obj("n2addr" -> Json.obj("rawData" -> obj.n2addr, "tokenized" -> tokenizeAddr(obj.n2addr)))
      jsonObj = jsonObj ++ Json.obj("n2data" -> Json.obj("rawData" -> rawN2Data, "processed" -> processData(rawN2Data)))
      //adding json object to json list
      jsonList = jsonObj :: jsonList
      println(Json.prettyPrint(jsonObj))
    }
    jsonList
  }

  def tokenizeObj(obj: Token) = {
    var jsonObj = Json.obj()
    val rawSupplierData = obj.supname + " " +obj.supaddr
    val rawConsigneeData = obj.conname + " " +obj.conaddr
    val rawN1Data = obj.n1name + " " +obj.n1addr
    val rawN2Data = obj.n2name + " " +obj.n2addr
    //adding Supplier Data
    jsonObj = jsonObj ++ Json.obj("supname" -> Json.obj("rawData" -> obj.supname, "tokenized" -> tokenizeName(obj.supname)))
    jsonObj = jsonObj ++ Json.obj("supaddr" -> Json.obj("rawData" -> obj.supaddr, "tokenized" -> tokenizeAddr(obj.supaddr)))
    jsonObj = jsonObj ++ Json.obj("supplier" -> Json.obj("rawData" -> rawSupplierData, "processed" -> processData(rawSupplierData)))
    //adding consignee data
    jsonObj = jsonObj ++ Json.obj("conname" -> Json.obj("rawData" -> obj.conname, "tokenized" -> tokenizeName(obj.conname)))
    jsonObj = jsonObj ++ Json.obj("conaddr" -> Json.obj("rawData" -> obj.conaddr, "tokenized" -> tokenizeAddr(obj.conaddr)))
    jsonObj = jsonObj ++ Json.obj("consignee" -> Json.obj("rawData" -> rawConsigneeData, "processed" -> processData(rawConsigneeData)))
    //adding n1 data
    jsonObj = jsonObj ++ Json.obj("n1name" -> Json.obj("rawData" -> obj.n1name, "tokenized" -> tokenizeName(obj.n1name)))
    jsonObj = jsonObj ++ Json.obj("n1addr" -> Json.obj("rawData" -> obj.n1addr, "tokenized" -> tokenizeAddr(obj.n1addr)))
    jsonObj = jsonObj ++ Json.obj("n1data" -> Json.obj("rawData" -> rawN1Data, "processed" -> processData(rawN1Data)))
    //adding n2 data
    jsonObj = jsonObj ++ Json.obj("n2name" -> Json.obj("rawData" -> obj.n2name, "tokenized" -> tokenizeName(obj.n2name)))
    jsonObj = jsonObj ++ Json.obj("n2addr" -> Json.obj("rawData" -> obj.n2addr, "tokenized" -> tokenizeAddr(obj.n2addr)))
    jsonObj = jsonObj ++ Json.obj("n2data" -> Json.obj("rawData" -> rawN2Data, "processed" -> processData(rawN2Data)))
    println(Json.prettyPrint(jsonObj))
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

  def tokenizeName(name: String): JsObject = {
    val tokenz = name.split(" ")
    var tokenMap: Map[String, Set[String]] = Map()
    var jsonList: List[JsObject] = List.empty[JsObject]
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
    val jsonData = getJson(tokenMap, name)
    jsonData
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

  def tokenizeAddr(addr: String): JsObject = {
    val tokenz = addr.split(" ")
    var tokenMap: Map[String, Set[String]] = Map()
    var jsonList: List[JsObject] = List.empty[JsObject]
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
    val jsonObj = getJson(tokenMap, addr)
    jsonObj
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

  def processData(rawData: String): JsObject = {
    val processedObj = CleanData.processData(rawData)
    var obj = Json.obj("name" -> processedObj.name, "country" -> processedObj.location.country, "city" -> processedObj.location.city,
              "state" -> processedObj.location.state, "pincode" -> processedObj.location.pinCode)
    obj
  }


}
