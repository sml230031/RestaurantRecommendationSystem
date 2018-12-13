package recommendationSystem
import java.io.{BufferedReader, InputStreamReader}

object Main extends App {
//
  val br = new BufferedReader(new InputStreamReader(System.in))
  println("Please enter your user ID: ")
  val input = br.readLine
  println("Please enter your city: ")
  val location = br.readLine
  println("System Processing...")
  ModelTraining.getRecsById(input.toInt, location.toString)
//  val businessDF = DataProcess.getBusinessDataFrame()
//  println(businessDF.count())
//  val business = businessDF.where(businessDF("business_id_INT").equalTo(402))
////  businessDF.show(2500)
//  business.show(100)

}
