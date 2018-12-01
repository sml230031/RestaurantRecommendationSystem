package recommendationSystem
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD

object RDD {


  val reviewDF = DataProcess.getReviewDataFrame()
  val userDF = DataProcess.getUserDataFrame()
  val businessDF = DataProcess.getBusinessDataFrame()
  var joined = reviewDF.join(userDF, Seq("user_id"),"left_outer").join(businessDF, Seq("business_id"),"left_outer")
    .select("user_id_INT","business_id_INT","user_id","business_id","stars")


  businessDF.createOrReplaceTempView("business")
  reviewDF.createOrReplaceTempView("review")


  val splits = reviewDF.randomSplit(Array(0.8, 0.2))
  val (trainingData, testData) = (splits(0), splits(1))

  def getTrainingRDD(): RDD[Rating] = {
    val trainingRdd = joined.rdd.map(row =>{
      val user_id_INT = row.getLong(0)
      val business_id_INT = row.getLong(1)
      val stars = row.getLong(4)
      Rating(business_id_INT.toInt, user_id_INT.toInt, stars.toFloat)
    }
    )
    trainingRdd
  }

  def getTestingRDD(): Unit = {
    val testingRdd = trainingData.rdd.map(row =>{
      val businessID = row.getString(0)
      val userID = row.getString(1)
      val stars = row.getString(2)
//      Rating(businessID, userID, stars.toFloat)
    }
    )
//    testingRdd
  }
}
