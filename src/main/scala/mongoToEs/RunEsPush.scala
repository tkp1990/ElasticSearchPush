
package mongoToEs

import tokenize.{Token, Tokenize, GetZPMainData}
/**
 * Created by kenneththomas on 4/6/16.
 */
object RunEsPush {
  def main(args: Array[String]) {
    //val obj = new GetZPMainData
    val obj = new Tokenize
    try{
      println("Application Start")
      //obj.getData()
      var tkList: List[Token] = List.empty[Token]
      val tk = Token("","a w faber-castell usa", "9450 allen drive cleveland oh 44125 usa.c/o lien tai trading co ltd 6/f well tech centre 9 pat tat street  san po kong kowloon hong kong",
      "target stores", "division of target corporation  1000 nicollet mall  minneapolis  mn 55403", "target customs broker  inc.", "33 south 6th st. cc-3300 minneapolis  mn 55402 attn: kathy collins tel: 612.304.4160",
          "-1", "-1")
      val tk1 = Token("","taiyo nippon sanso", "trading(shanghai)co. ltd.rm 1007 chamtime international finance center 1589 century avenue pudong shanghai 200122",
        "matheson tri-gas inc.6775 central", "avenue newark ca94560.u.s.a tel:1-510-7932559 related party:yes", "matheson tri-gas inc.6775 central", "avenue newark ca94560.u.s.a tel:1-510-7932559 related party:yes",
        "-1", "-1")
      val tk2 = Token("","encana", "3905-29th st. ne calgary  alberta canada attn:bruce tunna tel:+1-403-250-1806 ",
        "korea institute of geoscience and", "mineral resources (kigam)  30 gajungdong yusung-ku daejeon korea 305-350 attn:yi kyun kwon", "korea institute of geoscience and", "mineral resources (kigam)  30 gajungdong yusung-ku daejeon korea 305-350 attn:yi kyun kwon tel:+82-42-868-3394",
        "-1", "-1")
      tkList = tk :: tk1 :: tk2 :: tkList
      obj.tokenizeList(tkList)
      //obj.tokenizeObj(tk)
      //obj.tokenizeName("a w faber-castell u.s/a")
      println("Application End")
    } catch {
      case e: Exception => println("Exception: "+e.getMessage)
        e.printStackTrace()
    }
  }
}

