package mongoToEs

import tokenize.GetZPMainData
/**
 * Created by kenneththomas on 4/6/16.
 */
object RunEsPush {
  def main(args: Array[String]) {
    //val obj = new PushPartialData()
    val obj = new GetZPMainData
    try{
      println("Application Start")
      obj.getData()
      println("Application End")
    } catch {
      case e: Exception => println("Exception: "+e.getMessage)
    }
  }
}
