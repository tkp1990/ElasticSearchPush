package dataCleaning

/**
 * Created by kenneththomas on 4/14/16.
 */
object AddressExtraction {

  val SINGLE_LEVEL_TOKENS = "single"
  val DOUBLE_LEVEL_TOKENS = "double"
  val TRIPLE_LEVEL_TOKENS = "triple"
  val SEARCH_STATE = "state"
  val SEARCH_CITY = "city"
  val ZIPCODE_REGEX = """\s(\d{3})[-+,\s+]?(\d{2,3})(\W|$)([\s|$])?""".r

  def getCity(input: String): String = {
    var city = ""
    val in = input.toLowerCase()
    val tokenList1 = getTokens(in, SINGLE_LEVEL_TOKENS)
    city = city + searchForCity(tokenList1)
    val tokenList2 = getTokens(in, DOUBLE_LEVEL_TOKENS)
    val x = searchForCity(tokenList2)
    if(city.isEmpty)
      city = city + x
    else
      city = city + ", "+ x
    val tokenList3 = getTokens(in, TRIPLE_LEVEL_TOKENS)
    val y = searchForCity(tokenList3)
    if(city.isEmpty)
      city = city + y
    else
      city = city + ", "+ y
    city
  }

  def searchForCity(tokenList: List[String]): String = {
    var city = ""
    for(x <- tokenList) {
      if(DataMaps.cityMap.contains(x)){
        if(city.isEmpty)
          city = city + x
        else
          city = city + ", "+ x
      }
    }
    city
  }

  def getState(input: String): String = {
    var state = ""
    val in = input.toLowerCase()
    val tokenList1 = getTokens(in, SINGLE_LEVEL_TOKENS)
    state = state + searchForState(tokenList1)
    val tokenList2 = getTokens(in, DOUBLE_LEVEL_TOKENS)
    val x = searchForState(tokenList2)
    if(state.isEmpty)
      state = state + x
    else
      state = state + ", "+ x
    val tokenList3 = getTokens(in, TRIPLE_LEVEL_TOKENS)
    val y = searchForState(tokenList3)
    if(state.isEmpty && !y.isEmpty)
      state = state + y
    else if(!state.isEmpty && !y.isEmpty)
      state = state + ", "+ y
    state
  }

  def searchForState(tokenList: List[String]): String = {
    var state = ""
    for(x <- tokenList) {
      if(DataMaps.stateMap.contains(x)){
        if(state.isEmpty && !x.isEmpty)
          state = state + x
        else if(!state.isEmpty && !x.isEmpty)
          state = state + ", "+ x
      }
    }
    state
  }

  def getTokens(input: String, level: String): List[String] = {
    var tokens: List[String] = List.empty[String]
    val inList = input.split(" ")
    level match {
      case SINGLE_LEVEL_TOKENS =>
        tokens = inList.toList
      case DOUBLE_LEVEL_TOKENS =>
        for(x <- 0 to inList.length - 2){
          val st = inList(x).trim + " " + inList(x + 1).trim
          tokens = st :: tokens
        }
      case TRIPLE_LEVEL_TOKENS =>
        for(x <- 0 to inList.length - 3){
          val st = inList(x).trim + " " + inList(x + 1).trim + " " + inList(x + 2).trim
          tokens = st :: tokens
        }
    }
    tokens
  }

  def getCountry(str: String, searchAt: String): String = {
    var country = ""
    searchAt match {
      case SEARCH_CITY =>
        var cityList: List[String] = List.empty[String]
        if(str.contains(",")){
          cityList = str.split(",").toList
        } else {
          cityList = str :: cityList
        }
        country = getCountryFromCityMap(cityList)
      case SEARCH_STATE =>
        var stateList: List[String] = List.empty[String]
        if(str.contains(",")){
          stateList = str.split(",").toList
        } else {
          stateList = str :: stateList
        }
        country = getCountryFromStateMap(stateList)
    }
    println("Country getCountry: "+ country)
    country
  }

  def getCountryFromCityMap(list: List[String]): String = {
    var ctry = ""
    for(x <- list){
      if(DataMaps.cityMap.contains(x.trim)){
        val c = DataMaps.cityMap.get(x.trim)
        if(ctry.isEmpty && !c.isEmpty)
          ctry = ctry + c
        else if(!ctry.isEmpty && !c.isEmpty)
          ctry = ctry + ", " + c
      }
    }
    println("Country getCountryFromCityMap: "+ ctry)
    ctry
  }

  def getCountryFromStateMap(list: List[String]): String = {
    var ctry = ""
    for(x <- list){
      if(DataMaps.stateMap.contains(x.trim)){
        val c = DataMaps.stateMap.get(x.trim)
        if(ctry.isEmpty && !c.isEmpty)
          ctry = ctry + c
        else if(!ctry.isEmpty && !c.isEmpty)
          ctry = ctry + ", " + c
      }
    }
    println("Country getCountryFromStateMap: "+ ctry)
    ctry
  }

  def getZipcode(input: String): String = {
    var zip = ""
    val zipList = ZIPCODE_REGEX.findAllIn(input).toList
    for(x <- zipList) {
      if(zip.isEmpty && !x.isEmpty)
        zip = zip + x
      else if(!zip.isEmpty && !x.isEmpty)
        zip = zip + ", " + x
    }
    zip
  }

  def getCountryDirect(input: String): String = {
    var country = ""
    val tokens1 = getTokens(input, SINGLE_LEVEL_TOKENS)
    for(x <- tokens1) {
      if(DataMaps.countryMap.contains(" " + x.trim + " ")){
        if(country.isEmpty)
          country = country + x
        else
          country = country + ", " + x
      }
    }
    var tokens2 = getTokens(input, DOUBLE_LEVEL_TOKENS)
    for(x <- tokens1) {
      if(DataMaps.countryMap.contains(x.trim)){
        if(country.isEmpty && !x.isEmpty)
          country = country + x
        else if(!country.isEmpty && !x.isEmpty)
          country = country + ", " + x
      }
    }
    country
  }

  def getStreetAddr(input: String, loc: Location): String = {
    var in = input
    val in_ctry = in.replace(loc.country, "")
    val in_state = in_ctry.replace(loc.state, "")
    val in_city = in_state.replace(loc.city, "")
    val in_out = in_city.replace(loc.pinCode, "")
    in_out
  }
}
