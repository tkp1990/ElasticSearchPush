
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
      var tkList: List[Token] = getDataList()
      obj.tokenizeAndInsert(tkList)
      //obj.tokenizeObj(tk)
      //obj.tokenizeName("a w faber-castell u.s/a")
      println("Application End")
    } catch {
      case e: Exception => println("Exception: "+e.getMessage)
        e.printStackTrace()
    }
  }

  def getDataList(): List[Token] = {
    var tkList: List[Token] = List.empty[Token]
    val tk = Token("","a w faber-castell usa", "9450 allen drive cleveland oh 44125 usa.c/o lien tai trading co ltd 6/f well tech centre 9 pat tat street  san po kong kowloon hong kong",
      "target stores", "division of target corporation  1000 nicollet mall  minneapolis  mn 55403", "target customs broker  inc.", "33 south 6th st. cc-3300 minneapolis  mn 55402 attn: kathy collins tel: 612.304.4160",
      "-1", "-1")
    val tk1 = Token("","taiyo nippon sanso", "trading(shanghai)co. ltd.rm 1007 chamtime international finance center 1589 century avenue pudong shanghai 200122",
      "matheson tri-gas inc.6775 central", "avenue newark ca94560.u.s.a tel:1-510-7932559 related party:yes", "matheson tri-gas inc.6775 central", "avenue newark ca94560.u.s.a tel:1-510-7932559 related party:yes",
      "-1", "-1")
    val tk2 = Token("","encana", "3905-29th st. ne calgary  alberta canada attn:bruce tunna tel:+1-403-250-1806",
      "korea institute of geoscience and", "mineral resources (kigam)  30 gajungdong yusung-ku daejeon korea 305-350 attn:yi kyun kwon", "korea institute of geoscience and", "mineral resources (kigam)  30 gajungdong yusung-ku daejeon korea 305-350 attn:yi kyun kwon tel:+82-42-868-3394",
      "-1", "-1")
    val tk3 = Token("","ypf s.a", "777 pte r. saenz pena ave buenos aires ar",
      "to order", "vitol inc str suite 5500 1100 louisiana houstontx 77002 us ", "vitol inc", "1100 louisiana str suite 5500 houston tx 77002 us",
      "-1", "-1")
    val tk4 = Token("","petroleos de venezuela s.a", "edif petroleos de venezuela torre caracas 1010a ven tel 011-58-212-708-3508",
      "global companies llc", "800 south street waltham ma 02454 usa tel 781-398-4384", "global companies llc", "800 south street waltham ma 02454 usa tel 781-398-4384 ",
      "-1", "-1")
    val tk5 = Token("","china petroleum & chemical corp.", "shanghai gao qiao branch",
      "great lakes carbon llc", "4 greenspoint plaza suite 2200 16945 northchase drive houstontx 77060 ", "-1", "-1",
      "-1", "-1")
    val tk6 = Token("","repsol comercializadora de gas s.a.", "paseo de la castellana 280 madrid 28046  spain",
      "sempra lng marketing llc", "101 ash street  hq06d san diego  ca 92101 619-696-2744", "gac shipping (usa) inc.", "ams.usa@gac.com 484-953-3326",
      "-1", "-1")
    val tk7 = Token("","petrochina company", "world tower 16 andelu  xicheng district beijing 100011 china",
      "lukoil pan americas llc", "1095 avenue of the americas 33rd floor new york  ny 10036", "lukoil pan americas llc", "1095 avenue of the americas 33rd floor new york  ny 10036",
      "-1", "-1")
    val tk8 = Token("","time frame logistics nz ltd", "3unit 17a 203 kirkbride rd auckland nz",
      "chevron corporation", "6101 bollinger canyon room 3352 mary white san ramon ca 94583 us", "-1", "-1",
      "-1", "-1")
    val tk9 = Token("","indian oil corporation ltd", "g-9 ali yavar jung marg bandra (east) mumbaiin400051",
      "capital funding corp", "p.o. box no. 11922 salt lake city  utus84147 ", "jarvis international freight inc", "n/a 1950 south star point drive houston  texasus77023",
      "-1", "-1")
    val tk10 = Token("","reliance industries limited", "fibres marketing division 222 nariman point  4th floor bombay 400 021  india",
      "invista inc", "standard ontario whse 0967 d2 s. etiwanda ave ontario ca 91761 us purchase order no. 4800195846 release no. april releases", "bdp international", "dupont import operations 510 walnut street philadelphia pa 19106 usa",
      "-1", "-1")
    val tk11 = Token("","marubeni speciality chemicals inc", "10 bank street white plains  ny 10606 u.s.a.",
      "exxonmobil chemical company", "41501 wolvenine rd.shawnee ok 74804-9566 phone:(405)878-8279 fax:(405)878-8291 attn:mr.cheri ward", "-1", "-1",
      "-1", "-1")
    val tk12 = Token("","lg chem   ltd.", "20 yoido-dong  youngdungpo-gu lg twin towers seoul south korea 150-721",
      "to the order of", "lg chem america  inc. 17777 center court 675 cerritos ca 90703", "valero lp catoosa asphalt", "terminal 5575 e. channel road extension catoosa ok 74015",
      "-1", "-1")
    val tk13 = Token("","hindustan unilever limited", "hindustan lever house  165-166 backbay reclamation  mumbai 400 020  india. tel.: 39832066 ",
      "deep foods inc. (usa)", "363413-1090 springfield road  union (new jersey) nj 07083  usa", "deep foods inc. (usa)", "363413-1090 springfield road  union (new jersey) nj 07083  usa",
      "-1", "-1")
    val tk14 = Token("","nestle", "km 14.5 carretera roosevelt zona 3 mixco guatemala",
      "nestle usa inc", "800 north brand blcd. glendale  ca. 91203", "ups supply chain solutions", "1515 west 190th. st. suite 300 gardena  ca. 90248 attn: imports-nestle team",
      "-1", "-1")
    val tk15 = Token("","procter & gamble company (the)", "8500 governor's hill drive cincinnati oh 45249 united states 7742177 7742177",
      "procter & gamble company (the)", "8500 governor's hill drive cincinnati oh 45249 united states 7742177 7742177", "pbb global logistics", "suite 609 70 east sunrise highway valley stream ny 11581 united states",
      "-1", "-1")
    val tk16 = Token("","marubeni speciality chemicals inc", "10 bank street white plains  ny 10606 u.s.a.",
      "exxonmobil chemical company", "41501 wolvenine rd.shawnee ok 74804-9566 phone:(405)878-8279 fax:(405)878-8291 attn:mr.cheri ward", "-1", "-1",
      "-1", "-1")
    val tk17 = Token("","lg chem   ltd.", "20 yoido-dong  youngdungpo-gu lg twin towers seoul south korea 150-721",
      "to the order of", "lg chem america  inc. 17777 center court 675 cerritos ca 90703", "valero lp catoosa asphalt", "terminal 5575 e. channel road extension catoosa ok 74015",
      "-1", "-1")
    val tk18 = Token("","hindustan unilever limited", "hindustan lever house  165-166 backbay reclamation  mumbai 400 020  india. tel.: 39832066 ",
      "deep foods inc. (usa)", "363413-1090 springfield road  union (new jersey) nj 07083  usa", "deep foods inc. (usa)", "363413-1090 springfield road  union (new jersey) nj 07083  usa",
      "-1", "-1")
    val tk19 = Token("","nestle", "km 14.5 carretera roosevelt zona 3 mixco guatemala",
      "nestle usa inc", "800 north brand blcd. glendale  ca. 91203", "ups supply chain solutions", "1515 west 190th. st. suite 300 gardena  ca. 90248 attn: imports-nestle team",
      "-1", "-1")
    val tk20 = Token("","procter & gamble company (the)", "8500 governor's hill drive cincinnati oh 45249 united states 7742177 7742177",
      "procter & gamble company (the)", "8500 governor's hill drive cincinnati oh 45249 united states 7742177 7742177", "pbb global logistics", "suite 609 70 east sunrise highway valley stream ny 11581 united states",
      "-1", "-1")
    val tk21 = Token("","merck & co incorporated", "hertford road hoddesdon *fao sue loring* hertfordshire en11 9bu gb",
      "merck & co. incorporated", "attn: mr w.stuart  flint river park 3517 radium springs road albany ga 31705 us", "-1", "-1",
      "-1", "-1")
    val tk22 = Token("","l'oreal", "zi de moimont marly la ville  95670 fr",
      "l'oreal usa", "285 terminal avenue clark  nj 07066 us", "-1", "-1",
      "-1", "-1")
    val tk23 = Token("","walter garments corporation", "f.p. felix ave.  cainta rizal  philippines ",
      "the gap  inc", "1 harrison street san francisco  ca 94105 usa", "expeditors international", "5395 distriplex farms drive memphis  tn 38141 attn: myron watkins/gap department tel: 901.362.9771",
      "-1", "-1")
    val tk24 = Token("","macy's merchandising group llc", "11 penn plaza 9th floor new york ny",
      "hudson s bay company", "401 bay street 10th floor toronto ontario m5h2y4 toronto m5h 2y4  ca", "transpacific container terminal ltd", "lasalle  quebec . . canada montreal",
      "-1", "-1")
    val tk25 = Token("","levi strauss do brasil ind. e com", "av. portugal  no. 400 - compl. galpoes 6b itaqui itapevi sp 06690-110 br 11 3066-3677",
      "levi strauss co", "1155 battery street sao franciso ca 94111 us 1 415 501 6071", "-1", "-1",
      "-1", "-1")
    val tk26 = Token("","tefron ltd", "industrial center terodyon misgav 20179 israel 972 4 9900000",
      "alba waldensian", "john louis plant 720 main st. dock 975 valdese 28690 usa", "alba waldensian", "john louis plant 720 main st valdese nc 28690",
      "-1", "-1")
    val tk27 = Token("","industria de diseno textil  s.a.", "avda. de la diputacion-edificio inditex. 15142 arteixo  a coruna  spain tel.34981185409 fax.34981185454",
      "zara usa inc", "645 madison avenue  6th floor new york  ny 10022 tel.12123551415 fax.12127541128", "united customs services  inc", "1598 bw 82.av miami fl 33126 tel.13056399584 fax.13055943723 ctc: david whittingham",
      "-1", "-1")
    val tk28 = Token("","ikea", "spangatan 1 343 81 almhult sweden",
      "ikea san diego", "2149 fenton parkway san diego  ca 92108 usa", "ikea san diego", "2149 fenton parkway san diego  ca 92108 usa",
      "-1", "-1")
    val tk29 = Token("","euromarket designs inc", "311 half zcre road cranbury  nj 08512-000 usa",
      "euromarket designs inc", "import departmen  1250 techny road northbrook  il 60062  usa renaee hepp fax: 847 272 7397 tel: 847 272 2888", "expeditors international of", "washington.  inc. 150 raritan center parkway edison  nj 08837  person in charge: rui salgado tel:732 225 8670",
      "-1", "-1")
    val tk30 = Token("","unilever china ltd", "88 jin xiu av e&t development park hefei    cn ",
      "schenker (foods)", "200 chrysler drive brampton on l6s6g8  ca 9057904100", "-1", "-1",
      "-1", "-1")
    val tk31 = Token("","unilever  philippines", "1351 united nations avenue manila  philippines",
      "unilever hpc usa", "1 john street clinton  connecticut 06413  united states of america", "c.h. powel company", "2 hudson place 3rd floor hoboken  new jersey 07030 united states of america",
      "-1", "-1")

    tkList = tk :: tk1 :: tk2 :: tk3 :: tk4 :: tk5 :: tk6 :: tk7 :: tk8 :: tk9 :: tk10 :: tk11 :: tk12 :: tk13 :: tk14 :: tk15 :: tk16 :: tk17 :: tk18 :: tk19 :: tk20 :: tk21 :: tk22 :: tk23 :: tk24 :: tk25 :: tk26 :: tk27 :: tk28 :: tk29 :: tk30 :: tk31 :: tkList

    //tkList = tk :: tk1 :: tkList
    tkList
  }
}

