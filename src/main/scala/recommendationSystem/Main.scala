package recommendationSystem
import java.io.{BufferedReader, InputStreamReader}

object Main extends App {
//
  val br = new BufferedReader(new InputStreamReader(System.in))
  val input = br.readLine
  ModelTraining.getRecsById(input.toInt,"Las Vegas")
//  val businessDF = DataProcess.getBusinessDataFrame()
//  println(businessDF.count())
//  val business = businessDF.where(businessDF("business_id_INT").equalTo(402))
////  businessDF.show(2500)
//  business.show(100)

}
