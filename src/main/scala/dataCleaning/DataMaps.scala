package dataCleaning

import com.github.tototoshi.csv.CSVReader
import tokenize.SourceDir

import scala.collection.immutable.HashMap

/**
 * Created by kenneththomas on 3/4/16.
 */
object DataMaps {


  /*val countryMap: HashMap[String, String] = HashMap("kh" -> "Cambodia",
    "vn" -> "Vietnam",
    "colombo" -> "Sri Lanka",
    "hong kong" -> "Hong Kong",
    "hk" -> "Hong Kong",
    "h k" -> "Hong Kong",
    "lesotho" -> "Lesotho",
    "guatemala c a" -> "Guatemala",
    "guatemala" -> "Guatemala",
    "canada" -> "Canada",
    "china" -> "China",
    "taiwan" -> "Taiwan",
    "cambodia" -> "Cambodia",
    "india" -> "India",
    "India" -> "India",
    "in" -> "India",
    "ind" -> "India",
    "usa" -> "United States",
    "u s a" -> "United States",
    "u s" -> "United States",
    "us" -> "United States",
    "united states" -> "United States",
    "united states of america" -> "United States",
    "thailand" -> "Thailand",
    "korea" -> "South Korea",
    "kr" -> "South Korea"
  )*/

  /*val countryStateMap: HashMap[String, List[String]] = HashMap(
    "India" -> List("haryana", "punjab", "delhi"),
    "Cambodia" -> List("phnom penh"),
    "Vietnam" -> List("hung yen"),
    "Lesotho" -> List("maseru"),
    "Guatemala" -> List("san ignacio", "villa nueva"),
    "Hong Kong" -> List("kln","kowloon", "shantin", "wanchai", "tsuen wan", "tsuenwan", "yuen long kau hui", "yuenlong kauhui", "shatin", "tuen mun", "tuenmun", "taipo", "tai po"),
    "China" -> List("macau", "shenzhen","guangdong", "fujian","xin hui","xinhui", "qingdao","shanghai"),
    "Taiwan" -> List("taipei"),
    "Canada" -> List("toronto", "ontario", "alberta"),
    "United States of America" -> List(" pa ", "arkansas", " ca ", " nj ", " ny ", " tx ", " nv ", " fl ", " va ", "minneapolis", " mn "),
    "Thailand" -> List("chachoengsao"),
    "South Korea" -> List("kyong gi do"))*/


  /*val countryCityMap: HashMap[String, List[String]] = HashMap(
    "India" -> List("ludhiana", "gurgaon", "faridabad", "new delhi"),
    "United States" -> List("pittsburgh", "bentonville", "san francisco", "new york"),
    "Thailand" -> List("bangpakong"),
    "Taiwan" -> List("taichung", "tu-cheng", "tucheng", "nei hu","chang hwa", "nei-hu","neihu", "nei hu"),
    "China" -> List("dongguan", "xiamen", "quanzhou", "jiangmen"),
    "Hong Kong" -> List("tst", "tsim sha tsui", "tsimshatsui","cheung sha wan", "sanpokong","san po kong", "sheung shui", "sheungshui", "hunghom", "hung hom", "kwun tong","yau tong bay", "yau tong", "kwai chung", "tsuen wan", "wanchai","wan chai", "san po kong", "fotan", "fo tan", "kwau chung", "tai kok tsui"),
    "South Korea" -> List("seongnam", "daejeon"),
    "Canada" -> List("mississauga", "calgary")
  )*/

  val addressSplitWords: Set[String] = Set("suite", "room", "unit")

  var countryMap: Map[String, String] = Map()
  def populateCountryMap(path: SourceDir): Map[String, String] = {
    val reader = CSVReader.open(path.path)
    val it = reader.iterator
    while (it.hasNext) {
      val x = it.next()
      if(x.length > 0){
        try{
          countryMap ++= Map(x(0).trim.toLowerCase() -> x(0).trim.toLowerCase())
          countryMap ++= Map(x(1).trim.toLowerCase() -> x(0).trim.toLowerCase())
          countryMap ++= Map(x(2).trim.toLowerCase() -> x(0).trim.toLowerCase())
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
    //cMap.foreach(x => println(x))
    countryMap
  }

  var countryCityMap: Map[String, List[String]] = Map()
  def populateCitiesCountryMap(path: SourceDir) = {
    val reader = CSVReader.open(path.path)
    val it = reader.iterator
    while (it.hasNext) {
      val x = it.next()
      if(x.length > 0) {
        val key = x(1).trim.toLowerCase()
        if(countryCityMap.contains(key)){
          var l = countryCityMap.get(key).getOrElse(List.empty[String])
          val city = x(0).trim.toLowerCase
          l = city :: l
          countryCityMap = countryCityMap + (key -> l)
        } else {
          var l: List[String] = List.empty[String]
          val city = x(0).trim.toLowerCase
          l = city :: l
          countryCityMap = countryCityMap + (key -> l)
        }
      }
    }
    countryCityMap
  }

  var countryStateMap: Map[String, List[String]] = Map()
  def populateStateCountryMap(path: SourceDir) = {
    val reader = CSVReader.open(path.path)
    val it = reader.iterator
    while (it.hasNext) {
      val x = it.next()
      if( x.length > 0 ) {
        val key = x(1).trim.toLowerCase()
        if(countryStateMap.contains(key)){
          var l = countryStateMap.get(key).getOrElse(List.empty[String])
          val region = x(0).trim.toLowerCase
          l = region :: l
          countryStateMap = countryStateMap + (key -> l)
        } else {
          var l: List[String] = List.empty[String]
          val region = x(0).trim.toLowerCase
          l = region :: l
          countryStateMap = countryStateMap + (key -> l)
        }
      }
    }
    countryStateMap
  }

  var cityMap: Map[String, String] = Map()
  def populateCityMap(path: SourceDir) = {
    val reader = CSVReader.open(path.path)
    val it = reader.iterator
    while (it.hasNext) {
      val x = it.next()
      if(x.length > 0) {
        val key = x(0).trim.toLowerCase()
        if(!cityMap.contains(key)) {
          val ctry = x(1).trim.toLowerCase()
          cityMap = cityMap + (key -> ctry)
        }
      }
    }
    cityMap
  }

  var stateMap: Map[String, String] = Map()
  def populateStateMap(path: SourceDir) = {
    val reader = CSVReader.open(path.path)
    val it = reader.iterator
    while (it.hasNext) {
      val x = it.next()
      if(x.length > 0) {
        val key = x(0).trim.toLowerCase()
        if(!stateMap.contains(key)) {
          val ctry = x(1).trim.toLowerCase()
          stateMap = stateMap + (key -> ctry)
        }
      }
    }
    stateMap
  }
  /*var countryStateMap: Map[String, List[String]] = Map()
  def populateStateCountryMap(sqlContext: SQLContext, path: SourceDir) = {
    val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(path.path)
    val data = df.select("Country", "Region")
    for( x <- data ) {
      val key = x.getAs[String](1).trim.toLowerCase()
      if(countryStateMap.contains(key)){
        var l = countryStateMap.get(key).getOrElse(List.empty[String])
        val region = x.getAs[String](0).trim.toLowerCase
        l = region :: l
        countryStateMap = countryStateMap + (key -> l)
      } else {
        var l: List[String] = List.empty[String]
        val region = x.getAs[String](0).trim.toLowerCase
        l = region :: l
        countryStateMap = countryStateMap + (key -> l)
      }
    }
    countryStateMap
  }*/

}
