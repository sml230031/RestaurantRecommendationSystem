package recommendationSystem


import spray.json.DefaultJsonProtocol

import scala.io.Source

object DataFormat extends App {

  case class Business(business_id: String, name: String, categories: Option[String], state: String, city: String, address: String)
//  case class Business(business_id: String, name: String, categories: String, state: String, city: String, address: String)

  object DataFormatProtocol extends DefaultJsonProtocol {
    implicit val BusinessFormat = jsonFormat(Business, "business_id", "name",
      "categories", "state", "city", "address")
  }

  import DataFormatProtocol._
  import spray.json._

  var count = 0;
  val source = Source.fromFile("../finalproject/yelp-dataset/yelp_academic_dataset_business.json")
  val lineIterator = source.getLines()
  for(line <- lineIterator) {
    val businessObj = line.parseJson.convertTo[Business]
    count = count + 1
    println(count + " \"business_id\": " + businessObj.business_id + ", \"name\": " + businessObj.name +
      ", \"categories: \"" + businessObj.categories + ", \"state\": " + businessObj.state +
      ", \"city\": " + businessObj.city + ", \"address\": " + businessObj.address)
  }

}
