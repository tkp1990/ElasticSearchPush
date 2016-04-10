package tokenize

/**
 * Created by kenneththomas on 4/10/16.
 */
class Tokenize {

  val nameRegex1 = """[-,\,,.,!,']""".r
  val nameRegex2 = """[,,.,']""".r
  val stopTexts = """[.,]""".r
  val addrRegex1 = """[-,#.,,,+,!,',(,),/,\,&,%,$,*,@,:,`,?]""".r
  //val addrRegex2 = """[]""".r

  def tokenizeName(name: String): List[String] = {
    var nameList: List[String] = List()
    if(!(name == "" || name == "-1")){
      val n = FilterBusinessUnit.checkIfStringHasBusinessType(name) match {
        case Right(bType) => name.replace(bType, "")
        case Left(msg) => name
      }
      try{
        val n1 = nameRegex1.replaceAllIn(n, "").trim.toLowerCase()
        val _n2 = nameRegex2.replaceAllIn(n, "")
        val n2 = _n2.replace("-", " ").trim.toLowerCase()
        val n3 = stopTexts.replaceAllIn(n, "").trim.toLowerCase()
        nameList = n1 :: n2 :: n3 :: nameList
      } catch {
        case e: Exception =>
          println("Exception: Tokenize/tokenizeName -> "+e.getMessage)
          e.printStackTrace()
      }
    }
    nameList
  }

  def tokenizeAddr(addr: String): List[String] = {
    var addrList: List[String] = List()
    if(!(addr == "" || addr == "-1")){
      val a = addr
      try{
        val n1 = addrRegex1.replaceAllIn(a, "").trim.toLowerCase()
        val _n2 = a.replace("-", " ").trim.toLowerCase()
        val n2 = addrRegex1.replaceAllIn(_n2, " ")
        val n3 = a.trim.toLowerCase()
        addrList = n1 :: n2 :: n3 :: addrList
      } catch {
        case e: Exception => println("Exception:"+e.getMessage)
      }
    }
    addrList
  }


}
