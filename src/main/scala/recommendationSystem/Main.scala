package recommendationSystem

import java.io.{BufferedReader, InputStreamReader}

object Main extends App {
//
  val br = new BufferedReader(new InputStreamReader(System.in))
  val input = br.readLine
  ModelTraining.getRecsById(input.toInt)
//  val businessDF = DataProcess.getBusinessDataFrame()
//  val business = businessDF.where(businessDF("business_id_INT").equalTo(918080))
//  businessDF.show(2500)
//  business.show()

}
