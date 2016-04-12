package dataCleaning


/**
 * Created by kenneththomas on 3/8/16.
 */

import java.io.{FileWriter, BufferedWriter}
import api.mongo.MongoConfig
import play.api.libs.json.{JsString, JsObject, Json, JsValue}
import tokenize.SourceDir

case class SuplierInfo(suppinfo: String, corigin: String)

object MongoTest {
  val dataLimit = 200000;



  def main(args: Array[String]): Unit = {
    val mapDataUrl = SourceDir("./src/main/resources/CountryWithCodes.csv")
    //DataMaps.populateCountryMap(sqlContext, mapDataUrl)

    var skip = 0
    var limit = 500

    var counter = 0
    var lastId = ""
    //var data = collection.iterator
    while(counter <= dataLimit) {
      var jsonList: List[JsObject] = List[JsObject]()
      var outputArray: List[Array[String]] = List()
      /*val out = new BufferedWriter(new FileWriter("./src/main/resources/CSvTest/GlobalSourcingTransactionsOutPut"+counter+".csv"));
      val writer = new CSVWriter(out);
      val csvSchema = Array("Supplier Input","Supplier Name","Supplier Street", "Supplier City", "Supplier State", "Supplier Country", "Supplier Pincode", "Consignee Input", "Consignee Name","Consignee Street", "Consignee City", "Consignee State", "Consignee Country", "Consignee Pincode")
      writer.writeRow(csvSchema)*/
      val client = MongoConfig.getMongoClient("localhost", 27017)
      val collection = MongoConfig.getCollection("myDb", "myCollection", client)//getCollection("datacleaning", "ZPmainCollection")//
      val data = collection.find().skip(skip).limit(limit)
      skip = skip + limit
      counter = counter + limit
      try {
        data.foreach(x => {
          val json = Json.parse(x.toString);
          /*val suppName = (json \ "value" \ "supname").as[String]
          val supAddr = (json \ "value" \ "supaddr").as[String]
          val conName = (json \ "value" \ "conname").as[String]
          val conAddr = (json \ "value" \ "conaddr").as[String]
          val n1Name = (json \ "value" \ "n1name").as[String]
          val n1Addr = (json \ "value" \ "n1addr").as[String]
          val n2Name = (json \ "value" \ "n2name").as[String]
          val n2Addr = (json \ "value" \ "n2addr").as[String]
          val supplier = suppName + " " + supAddr
          val consignee = conName + " " + conAddr
          val n1 = n1Name + n1Addr
          val n2 = n2Name + n2Addr
          */
          val supplier = (json \ "suppinfo").as[String]
          val consignee = (json \ "coninfo").as[String]
          val supplierObj = CleanData.processData(supplier)
          val consigneeObj = CleanData.processData(consignee)
          /*val n1Obj = CleanData.processData(n1)
          val n2Obj = CleanData.processData(n2)*/
          val jsObject: JsObject = Json.obj(
            "supplier" -> Json.obj("input" -> JsString(supplier),"name" -> JsString(supplierObj.name), "street" -> JsString(supplierObj.street), "city" -> JsString(supplierObj.location.city),
              "state" -> JsString(supplierObj.location.state), "country" -> JsString(supplierObj.location.country), "pincode" -> JsString(supplierObj.location.pinCode)),
            "consignee" -> Json.obj("input" -> JsString(consignee),"name"-> JsString(consigneeObj.name), "street" -> JsString(consigneeObj.street), "city" -> JsString(consigneeObj.location.city),
              "state" -> JsString(consigneeObj.location.state), "country" -> JsString(consigneeObj.location.country), "pincode" -> JsString(consigneeObj.location.pinCode))
          )
          jsonList = jsObject :: jsonList
          //CleanData.processData(conSignee)
          val rowData: Array[String] = Array(supplier, supplierObj.name, supplierObj.street, supplierObj.location.city, supplierObj.location.state,
            supplierObj.location.country, supplierObj.location.pinCode,
            consignee, consigneeObj.name, consigneeObj.street, consigneeObj.location.city, consigneeObj.location.state,
            consigneeObj.location.country, consigneeObj.location.pinCode)
          outputArray ::= rowData.asInstanceOf[Array[String]]
        })
        for(x <- outputArray){
          //bulkInsert(jsonList, collection)
          //writer.writeRow(x.asInstanceOf[Array[String]])
        }
        //out.close()
      } catch {
        case e: Exception =>
          println("Exception Occurred while looping through Data Frame")
          println(e.getMessage)
      }finally {
        client.close()
      }
    }
  }

}
