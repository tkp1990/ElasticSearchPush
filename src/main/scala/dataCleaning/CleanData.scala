package dataCleaning


import scala.collection.immutable.HashMap

/**
 * Created by kenneththomas on 3/4/16.
 */
object CleanData {

  private val stop_text_list = """[-,#.,,,+,!,',(,),/,\,&,%,$,*,@,:,`,?]""".r
  private val zipIN="""\s(\d{3})[-+,\s+]?(\d{2,3})(\W|$)([\s|$])?""".r
  private val addrRegex = """[a-zA_Z]*[0-9]+[a-zA-Z0-9_.-/]*""".r

  final val cityPecWt = 23.0
  final val statePecWt = 17.0
  final val countryPecWt = 12.0
  final val streetPecWt = 10.0
  final val pincodePecWt = 10.0
  final val businessTypePecWt = 7.0
  final val companyNamePecWt = 25.0

  val businessTypeMap: HashMap[String, String] = HashMap("ltd" -> "LTD", "limited" -> "Limited", "pvt ltd" -> "Private Limited",
    "india pvt ltd" -> "India Private Limited", "llp" -> "LLP", "inc" -> "Inc", "co" -> "cooperated","corp" -> "cooperated", "llc" -> "LLC",
    "limitada" -> "Limited","co limit" -> "Co Limited", "co limited" -> "Co Limited", "co ltd" -> "Co Limited", "pty ltd" -> "Pty Limited",
    "s a " -> "SA", "sa" -> "SA")

  val stopWordList = Set("center", "central", "century", "east", "south", "north", "west", "hill", "court", "street", "tower", "city")

  def processData(input: String): SellerObj = {
    val a = stop_text_list.replaceAllIn(input, "")
    val in1 = a.replace("(", " ( ")
    val bracesInput = in1.replace(")", " ) ")

    val tokenz = splitInput(bracesInput)
    var _companyName, _finalCompanyName, _businessType, _st = ""
    checkIfStringHasBusinessType(bracesInput) match {
      case Right(bType) =>
        val in = bracesInput.split(bType)
        if(in.length > 1){
          val stopFilteredInput = removeStopWords(in(1))
          val loc = getAddrDetails(stopFilteredInput)
          _st = filterLocationDetails(in(1), loc)
//          Logger.debug("Country: "+loc.country+" State: "+loc.state+" City: "+loc.city+" pincode: "+loc.pinCode)
//          Logger.debug("CompanyName: "+in(0)+"Street Data: "+ _st)
          val obj = assignPointsForSupplierObj(in(0) ,_st, loc, bType, "")
          return obj
        }else  {
          val loc = getAddrDetails(bracesInput)
          val remainingString = filterLocationDetails(bracesInput, loc)
          val (_c, _s) = getCompNameWhenNoBusinessType(remainingString)
          _companyName = _c; _st = _s
//          Logger.debug("Country: "+loc.country+" State: "+loc.state+" City: "+loc.city+" pincode: "+loc.pinCode)
//          Logger.debug("Remaining String: "+remainingString)
//          Logger.debug("CompanyName: "+_companyName+" St: "+ _st)
          val obj = assignPointsForSupplierObj(_companyName ,_st, loc, "", "")
          return obj
        }
      case Left(msg) =>
        val (_c, _s) = getCompNameWhenNoBusinessType(bracesInput)
        _companyName = _c; _st = _s
        val stopFilteredInput = removeStopWords(_st)
        val loc = getAddrDetails(stopFilteredInput)

        val remainingString = filterLocationDetails(bracesInput, loc)
//        Logger.debug("Country: "+loc.country+" State: "+loc.state+" City: "+loc.city+" pincode: "+loc.pinCode)
//        Logger.debug("Remaining String: "+remainingString)
//        Logger.debug("CompanyName: "+_companyName+" St: "+ remainingString)
        val obj = assignPointsForSupplierObj(_companyName ,remainingString, loc, "", "")
        return obj
    }
  }

  def removeStopWords(input: String): String = {
    var in = input
    val tokens = input.toLowerCase.split(" ")
    for(x <- tokens) {
      if(stopWordList.contains(x)) {
        in = in.replace(x , "")
      }
    }
    in
  }

  /**
   *
   * @param input - the input string which contains the supplier name and address
   * @return - the Business Type
   */
  def checkIfStringHasBusinessType(input: String): Either[String, String] = {
    val tempInput = stop_text_list.replaceAllIn(input, "")
    //println("TempInput: "+ tempInput)
    val tokenz = tempInput.split(" ").toList
    checkIfBusniessTypeIsMultiText(tokenz) match {
      case Right(bType) =>
        return Right(bType)
      case Left(msg) =>
        for ( x <- tokenz ) {
          businessTypeMap.contains(x.toLowerCase()) match {
            case true =>
              //println("Single Match true: "+x)
              return Right(x)
            case false =>
              Left("")
          }
        }
    }
    Left("No Business Entity Present in String Or our Map doesn't have this particular one")
  }

  /**
   *
   * @param input - List of each individual token in the input string
   * @return - the Business entity if length > 1
   */
  def checkIfBusniessTypeIsMultiText(input: List[String]): Either[String, String] = {
    for( x <- 0 to input.length - 2) {
      val searchTerm = input(x) + " " + input(x + 1)
      //println(searchTerm)
      if(businessTypeMap.contains(searchTerm)) {
        businessTypeMap.get(searchTerm) match {
          case Some(bType) =>
            return Right(searchTerm)
          case None => Left("")
        }
      }
    }
    Left("")
  }

  def getStartOfPossibleAddress(input: String): Either[String, String] = {
    val divider = addrRegex.findAllMatchIn(input).toList
    for(x <- divider){
      val a = input.split(x.toString())
      if(a.length > 1){
        if(!a(0).isEmpty) return Right(x.toString())
      }
    }
    Left("")
  }

  def getCompNameWhenNoBusinessType(input: String):(String,String) = {
    getStartOfPossibleAddress(input) match {
      case Right(divider) =>
        val ret = input.split(divider)
        if(ret.length > 1){
          return (ret(0), divider + ret(1))
        }else{
          ("","")
        }
      case Left(msg) => ("","")
    }
  }


  def getAddressDetails(input: String): Location = {
    var c1, c2 = ""
    val city = AddressExtraction.getCity(input)
    val state = AddressExtraction.getState(input)
    if(!city.isEmpty)
      c1 = AddressExtraction.getCountry(input, AddressExtraction.SEARCH_CITY)
    if(!state.isEmpty)
      c2 = AddressExtraction.getCountry(input, AddressExtraction.SEARCH_STATE)
    val country = {
      if(!c1.isEmpty && !c2.isEmpty)
        c1 + ", " + c2
      else if(c2.isEmpty && !c1.isEmpty)
        c1
      else if(c1.isEmpty && !c1.isEmpty)
        c2
      else
        ""
    }
    val pincode = AddressExtraction.getZipcode(input)
    Location(country, "", state, city, pincode)
  }


  /**
   *
   * @param input - Input string containg country statecity pincode and street
   * @return - Location object
   */
  def getAddrDetails(input: String): Location = {
    val tokenz = input.split(" ").toList
    var country, pincode, city, state, inCtry = ""
    for (x <- tokenz) {
      if (DataMaps.countryMap.contains(x.toLowerCase())) {
        DataMaps.countryMap.get(x.toLowerCase()) match {
          case Some(ctry) =>
            country = ctry
            inCtry = x
            val (city, state) = getStateAndCity(input, ctry)
          //println("Country:  "+ctry+" State: "+state+" City: "+city)
          //return Right(ctry,x)
          case None =>
            val (_city, _state) = getStateAndCity(input, "")
            city = _city; state = _state
            //logger
            println("Country:  "+" No Data available "+" State: "+state+" City: "+city)
          //Left("")
        }
      }
      pincode = filterPincode(input) match {
        case Right(pin) => pin
        case Left(msg) => ""
      }
    }
    val (_city, _state) = getStateAndCity(input, "")
    city = _city; state = _state
    country.isEmpty match {
      case true =>
        val a: String = multiTokenSearchInIndexForCountry(input) match{
          case Right(inC1) =>
            inC1
          case Left(msg) => ("")
        }
        inCtry = a
      case false =>
      //Nothing
    }
    //println("Country:  "+" No Data available "+" State: "+state+" City: "+city+" Pincode: "+pincode)
    Location( country.toLowerCase().trim, inCtry, state.trim, city.trim, pincode.trim )
  }

  /**
   * Try and extract countries that have a space in between them.
   * @param in
   * @return - Either country extracted from the input string or an error message
   */
  def multiTokenSearchInIndexForCountry(in: String): Either[String, String] = {
    val input = in.toLowerCase().split(" ").toList
    for( x <- 0 to input.length - 2) {
      val searchTerm = input(x) + " " + input(x + 1)
      //println("SearchTerm -> "+searchTerm)
      if (DataMaps.countryMap.contains(searchTerm.trim)) {
        return DataMaps.countryMap.get(searchTerm.trim) match {
          case Some(country) => Right(searchTerm)
          case _ => Left("in multi text search")
        }
      }
    }
    val x = checkAgainstAllCountires(in)
    if(!x.isEmpty)
      return Right(x)
    Left("")
  }

  def checkAgainstAllCountires(input: String): String = {
    var ctry = ""
    val countrySet = DataMaps.countryMap.keys.toSet
    for(x <- countrySet) {
      if(input.contains(" "+x+" "))
        ctry = x
    }
    ctry
  }

  /**
   * Get state and city details from the input passed, using countryStateMap and countryCityMap
   * @param input
   * @param country
   * @return
   */
  def getStateAndCity(input: String, country:String): (String, String) = {
    val city = filterWithCountryAndCity(input, country) match {
      case Right(_city) => _city
      case Left(msg) => ""
    }
    val state = filterWithCountryAndState(input, country) match {
      case Right(_state) => _state
      case Left(msg) => ""
    }
    (city, state)
  }

  /**
   * Get city details from the input passed, using countryCityMap and country.
   * If country is empty search all city entries for a match.
   * @param input
   * @param country
   * @return
   */
  def filterCountryAndCity(input: String,country:String): Either[String,String] ={
    DataMaps.countryCityMap.get(country) match {
      case Some(cities) =>
        val tokenz = input.split(" ")
        for( x <- tokenz ){
          for(city <- cities) {
            if(x.equals(city)) return Right(x)
          }
        }

        for(city <- cities){
          val c = city + " "
          val cityData = c.r.findFirstIn(input)
          if (!cityData.isEmpty) return Right(cityData.get)
        }
      case None =>
        compareWithAllCities(input)
    }
    if(country.isEmpty)
      return compareWithAllCities(input)
    Left("")
  }

  def compareWithAllCities(input: String): Either[String, String] = {
    val citiesList = DataMaps.countryCityMap.values.toList
    val citiesSet = citiesList.flatMap(x => x).toSet
    val in = input.split(" ").toList
    /*for( x <- 0 to in.length - 2) {
      val searchTerm = in(x) + " " + in(x + 1)
      if(citiesSet.contains(searchTerm)) return Right(searchTerm)
    }*/
    for( x <- in ){
      if (citiesSet.contains(x) && x.length > 3) return Right(x)
    }
    for(a <- citiesSet) {
      val city = a+" ".r.findFirstIn(input)
      if(!city.isEmpty && a.length > 3)
        return Right(a)
    }

    Left("")
  }

  def filterWithCountryAndCity(input: String, country:String) = {
    var cityRet = ""
    var cityRetSet: Set[String] = Set.empty[String]
    DataMaps.countryCityMap.get(country) match {
      case Some(citiesList) =>
        val tokenz = input.split(" ")
        for( city <- citiesList) {
          val c = city + " "
          val cityData = c.r.findFirstIn(input)
          if (!cityData.isEmpty){
            if(cityRet.isEmpty && !cityRetSet.contains(cityData.get)){
              cityRet = cityRet + cityData.get
              cityRetSet = cityRetSet + cityData.get
            }
            else if(!cityRet.isEmpty && !cityRetSet.contains(cityData.get)){
              cityRet = cityRet + ", " +  cityData.get
              cityRetSet = cityRetSet + cityData.get
            }
          }
        }
      case _ =>
        compareWithAllCities(input) match {
          case Right(city) =>
            if(cityRet.isEmpty && !cityRetSet.contains(city)){
              cityRet = cityRet + city
              cityRetSet = cityRetSet + city
            }
            else if(!cityRet.isEmpty && !cityRetSet.contains(city)){
              cityRet = cityRet + ", " +  city
              cityRetSet = cityRetSet + city
            }
          case Left(msg) =>

        }
    }
    if(country.isEmpty){
      compareWithAllCities(input) match {
        case Right(city) =>
          if(cityRet.isEmpty && !cityRetSet.contains(city)){
            cityRet = cityRet + city
            cityRetSet = cityRetSet + city
          }
          else if(!cityRet.isEmpty && !cityRetSet.contains(city)){
            cityRet = cityRet + ", " +  city
            cityRetSet = cityRetSet + city
          }
        case Left(msg) =>
      }
    }
    if(cityRet.isEmpty)
      Left("")
    else
      Right(cityRet)
  }

  def filterWithCountryAndState(input: String, country: String): Either[String,String] = {
    var stateRet = ""
    var stateRetSet: Set[String] = Set.empty[String]
    DataMaps.countryStateMap.get(country) match {
      case Some(stateList) =>
        for(state <- stateList) {
          val s = " " + state + " "
          val stateData = s.r.findFirstIn(input)
          if (!stateData.isEmpty) {
            if(stateRet.isEmpty && !stateRetSet.contains(stateData.get)){
              stateRet = stateRet + stateData.get
              stateRetSet = stateRetSet + stateData.get
            }
            else if(!stateRet.isEmpty && !stateRetSet.contains(stateData.get)){
              stateRet = stateRet + ", " + stateData.get
              stateRetSet = stateRetSet + stateData.get
            }
          }
        }
      case _ =>
        compareWithAllStates(input) match {
          case Right(sData) =>
            if(stateRet.isEmpty && !stateRetSet.contains(sData)){
              stateRet = stateRet + sData
              stateRetSet = stateRetSet + sData
            }
            else if(!stateRet.isEmpty && !stateRetSet.contains(sData)){
              stateRet = stateRet + ", " + sData
              stateRetSet = stateRetSet + sData
            }
          case Left(msg) =>
        }
    }
    if(country.isEmpty){
      compareWithAllStates(input) match {
        case Right(sData) =>
          if(stateRet.isEmpty && !stateRetSet.contains(sData)){
            stateRet = stateRet + sData
            stateRetSet = stateRetSet + sData
          }
          else if(!stateRet.isEmpty && !stateRetSet.contains(sData)){
            stateRet = stateRet + ", " + sData
            stateRetSet = stateRetSet + sData
          }
        case Left(msg) =>
      }
    }
    if(stateRet.isEmpty)
      Left("")
    else
      Right(stateRet)
  }

  /**
   * Get state details from the input passed, using countryStateMap and country.
   * If country is empty search all state entries for a match.
   * @param input
   * @param country
   * @return
   */
  def filterCountryAndState(input: String,country:String): Either[String,String] ={
    DataMaps.countryStateMap.get(country) match {
      case Some(states) =>
        for(state <- states){
          val stateData = state.r.findFirstIn(input)
          if (!stateData.isEmpty) return Right(stateData.get)
        }
      case None =>
        compareWithAllStates(input)
    }
    if(country.isEmpty)
      return compareWithAllStates(input)
    Left("")
  }

  def compareWithAllStates(input: String): Either[String,String] ={
    val statesList = DataMaps.countryStateMap.values.toList
    val stateSet = statesList.flatMap(x => x).toSet
    val in = input.split(" ").toList
    for( x <- in ){
      if (stateSet.contains(x) && x.length > 3) return Right(x)
    }
    for(a <- stateSet) {
      val s = a + " "
      val state = s.r.findFirstIn(input)
      if(!state.isEmpty && a.length > 3)
        return Right(a)
    }
    Left("")
  }

  /**
   * Using zip regex extract the zipcode details from the input passed
   * @param input
   * @return
   */
  def filterPincode(input:String):Either[String,String]={
    var pincode =zipIN.findFirstIn(input)
    if(pincode nonEmpty) return Right(pincode.get)
    else Left("")
  }

  /**
   * Extract the location details from the input and return whats remaining.
   * @return
   */
  def filterLocationDetails(input: String, loc: Location): String = {
    var in = input
    in = in.replace(loc.inCtry, "")
    in = in.replace(loc.city, "")
    in = in.replace(loc.state, "")
    in = in.replace(loc.pinCode, "")
    in
  }

  def assignPointsForSupplierObj(companyName: String,  st: String, loc: Location, bType: String, remainingSt: String): SellerObj = {
    var points = 0.0
    if (!loc.country.isEmpty) {
      val countriesList = DataMaps.countryMap.values.toSet
      if (countriesList.contains(loc.country))
        points += countryPecWt

      if (!loc.state.isEmpty) {
        val statesList = DataMaps.countryStateMap.get(loc.country)
        statesList match {
          case Some(stateList) =>
            val stateSet = stateList.toSet
            if (stateSet.contains(loc.state))
              points += statePecWt
          case None => points += 0
        }
        //points += statePecWt
      }
      if (!loc.city.isEmpty) {
        val cityList = DataMaps.countryCityMap.get(loc.country)
        cityList match {
          case Some(cityL) =>
            val citySet = cityL.toSet
            if (citySet.contains(loc.city))
              points += cityPecWt
          case None =>
            points += 0
        }
      }
    } else {
      if (!loc.state.isEmpty) {
        points += statePecWt
      }
      if (!loc.city.isEmpty) {
        points += cityPecWt
      }
    }
    if (!st.isEmpty) points += streetPecWt
    if (!loc.pinCode.isEmpty) {
      points += pincodePecWt
    }
    if (!bType.isEmpty) {
      if (businessTypeMap.contains(bType))
        points += businessTypePecWt
    }
    if (!companyName.isEmpty) points += companyNamePecWt
    println("Point: "+points+" Company : "+ companyName)
    val _cmpName = {
      var c = companyName
      val bTypeList = businessTypeMap.keys.toSet
      for(x <- bTypeList) {
        val a = " " + x + " "
        c = c.replaceAll(a, "")
      }
      c
    }
    SellerObj(_cmpName, st, loc, bType, remainingSt, points)
  }

  /**
   * Helper Functions
   */

  def splitInput(input: String): List[String] = {
    input.toLowerCase().split(" ").toList
  }

}
