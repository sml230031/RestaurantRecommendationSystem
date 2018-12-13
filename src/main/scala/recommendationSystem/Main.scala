package recommendationSystem
import java.io.{BufferedReader, InputStreamReader}

object Main extends App {
//
  val br = new BufferedReader(new InputStreamReader(System.in))
  val input = br.readLine
  val location = br.readLine
  ModelTraining.getRecsById(input.toInt, location.toString)
//  val businessDF = DataProcess.getBusinessDataFrame()
//  println(businessDF.count())
//  val business = businessDF.where(businessDF("business_id_INT").equalTo(402))
////  businessDF.show(2500)
//  business.show(100)

}
