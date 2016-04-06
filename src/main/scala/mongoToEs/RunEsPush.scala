package mongoToEs

/**
 * Created by kenneththomas on 4/6/16.
 */
class RunEsPush {
  def main(args: Array[String]) {
    val obj = new EsJavaApi()
    try{
      obj.getData()
    } catch {
      case e: Exception => println("Exception: "+e.getMessage)
    }
  }
}
