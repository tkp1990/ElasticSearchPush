package dataCleaning

/**
 * Created by kenneththomas on 4/12/16.
 */
class SellerObject {

}

case class SellerObj(name: String, street: String, location:Location, businessType: String, remainingString: String, accScore: Double) {
}

case class Location(country: String, inCtry: String, state: String, city: String, pinCode: String){
}
