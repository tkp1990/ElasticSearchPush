package tokenize

import scala.collection.immutable.HashMap

/**
 * Created by kenneththomas on 4/10/16.
 */
object FilterBusinessUnit {


  val businessTypeMap: HashMap[String, String] = HashMap("ltd" -> "LTD", "limited" -> "Limited", "pvt ltd" -> "Private Limited",
    "india pvt ltd" -> "India Private Limited", "llp" -> "LLP", "inc" -> "Inc", "co" -> "cooperated","corp" -> "cooperated", "llc" -> "LLC",
    "limitada" -> "Limited","co limit" -> "Co Limited", "co limited" -> "Co Limited", "co ltd" -> "Co Limited", "pty ltd" -> "Pty Limited",
    "s a " -> "SA", "sa" -> "SA")
  val stop_text_list = """[-,\,,.,!,']""".r
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
}
