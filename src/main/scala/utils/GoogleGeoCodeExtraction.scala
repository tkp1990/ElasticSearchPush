package utils

import java.net.URLEncoder

import akka.util.Timeout

import scala.concurrent.Await

//import play.api.libs.ws.WS
/**
 * Created by kenneththomas on 4/19/16.
 */
class GoogleGeoCodeExtraction {

  /*import scala.language.postfixOps

  val builder = new com.ning.http.client.AsyncHttpClientConfig.Builder()
  val client = new play.api.libs.ws.ning.NingWSClient(builder.build())
  val response = client.url(url).get()

  def fetchLatitudeAndLongitude(address: String): Option[(Double, Double)] = {
    implicit val timeout = Timeout(50000 milliseconds)

    // Encoded the address in order to remove the spaces from the address (spaces will be replaced by '+')
    //@purpose There should be no spaces in the parameter values for a GET request
    val addressEncoded = URLEncoder.encode(address, "UTF-8")
    val jsonContainingLatitudeAndLongitude = ws.url("https://maps.googleapis.com/maps/api/geocode/json?address=" + addressEncoded + "&sensor=true").get()

    val future = jsonContainingLatitudeAndLongitude map {
      response => (response.json \\ "location")
    }

    // Wait until the future completes (Specified the timeout above)

    val result = Await.result(future, timeout.duration).asInstanceOf[List[play.api.libs.json.JsObject]]

    if (result.isEmpty) None else {
      //Fetch the values for Latitude & Longitude from the result of future
      val latitude = (result(0) \\ "lat")(0).toString.toDouble
      val longitude = (result(0) \\ "lng")(0).toString.toDouble
      println(s"Latitude and longitude - ($latitude, $longitude) for address - $address")
      Some((latitude, longitude))
    }
  }

  def fetchLatitudeAndLongitudeAsObject(address: String): Option[LatitudeLongitude] = {
    fetchLatitudeAndLongitude(address).map(x => LatitudeLongitude(latitude = x._1, longitude = x._2))
  }*/
}

case class LatitudeLongitude(latitude: Double, longitude: Double)
