package recommendationSystem

import spray.json.DefaultJsonProtocol
import java.text.SimpleDateFormat
import scala.collection.mutable.HashMap
import scala.io.Source

object DataFormat {

  case class Business(business_id: String, name: String, categories: Option[String], state: String, city: String, address: String)

  case class Review(user_id: String, business_id: String, stars: Int, date: String)

  case class User(user_id: String, review_count: Int, useful: Int)

  object BusinessProtocol extends DefaultJsonProtocol {
    implicit val BusinessFormat = jsonFormat(Business, "business_id", "name",
      "categories", "state", "city", "address")
  }

  object ReviewProtocol extends DefaultJsonProtocol {
    implicit val ReviewFormat = jsonFormat(Review, "user_id", "business_id",
      "stars", "date")
  }

  object UserProtocol extends DefaultJsonProtocol {
    implicit val UserFormat = jsonFormat(User, "user_id", "review_count", "useful")
  }

  def getBusinessData(filePath: String): HashMap[String, Business] = {

    import BusinessProtocol._
    import spray.json._

    val source = Source.fromFile(filePath)
    val lineIterator = source.getLines()
    val businessMap: HashMap[String, Business] = new HashMap[String, Business]()
    for (business <- lineIterator) {
      val businessObj: Business = business.parseJson.convertTo[Business]
      businessMap.put(businessObj.business_id, businessObj)
    }
    return businessMap
  }


  def getReviewData(filePath: String): HashMap[String, Review] = {

    import ReviewProtocol._
    import spray.json._

    val source = Source.fromFile(filePath)
    val lineIterator = source.getLines()
    val reviewMap: HashMap[String, Review] = new HashMap[String, Review]()
    var count = 0

    for (review <- lineIterator) {
      val reviewObj: Review = review.parseJson.convertTo[Review]

      val fmt = new SimpleDateFormat("yyyy-MM-dd")
      val reviewDate = fmt.parse(reviewObj.date)

      val date = new SimpleDateFormat("yyyy-MM-dd").parse("2015-01-01")
      if(reviewDate.after(date)) {
        count = count + 1
        reviewMap.put(reviewObj.user_id + "|" +reviewObj.business_id, reviewObj)
        if(count % 1000 == 0)println(count)
      }
    }
    return reviewMap
  }


  def getUserData(filePath: String): Map[String, User] = {
    import UserProtocol._
    import spray.json._

    val source = Source.fromFile(filePath)

    val lineIterator = source.getLines()

    var map: Map[String, User] = Map()

    val users = for (user <- lineIterator) yield user.parseJson.convertTo[User]

    for (user <- users) {
      map += (user.user_id -> user)
    }
    map
    }
}
