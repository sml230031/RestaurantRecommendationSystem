package recommendationSystem

import java.text.SimpleDateFormat

import spray.json.DefaultJsonProtocol

import scala.collection.mutable.HashMap
import scala.io.Source

object DataFormat {

  case class Business(business_id: String, name: String, categories: Option[String], state: String, city: String, address: String)

  case class RawReview(user_id: String, business_id: String, stars: Int, date: String)

  case class Review(user_id: String, business_id: String, stars: Int)

  object BusinessProtocol extends DefaultJsonProtocol {
    implicit val BusinessFormat = jsonFormat(Business, "business_id", "name",
      "categories", "state", "city", "address")
  }

  object ReviewProtocol extends DefaultJsonProtocol {
    implicit val ReviewFormat = jsonFormat(RawReview, "user_id", "business_id",
      "stars", "date")
  }

  def getBusinessData(filePath: String): HashMap[String, Business] = {

    import BusinessProtocol._
    import spray.json._

    val source = Source.fromFile(filePath)
    val lineIterator = source.getLines()
    val businessMap: HashMap[String, Business] = new HashMap[String ,Business]()
    for(business <- lineIterator) {
      val businessObj: Business = business.parseJson.convertTo[Business]
      businessMap.put(businessObj.business_id, businessObj)
    }
    return businessMap
  }



}
