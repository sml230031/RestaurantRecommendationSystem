package recommendationSystem
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object RDD {

  val reviewDF = DataProcess.getReviewDataFrame()
  val userDF = DataProcess.getUserDataFrame()
  val businessDF = DataProcess.getBusinessDataFrame()

  val joined: DataFrame = reviewDF.join(userDF, Seq("user_id"),"left_outer")
    .join(businessDF, Seq("business_id"),"left_outer")
    .select("user_id_INT","business_id_INT", "user_id", "business_id", "stars")

  //  userDF.write.format("csv").mode("overwrite").save("template/userJunk")

  //filter business_id_INT doesn't contain number
  val joinedFilterNull = joined.filter(joined("business_id_INT").rlike("\\d+"))

  businessDF.createOrReplaceTempView("business")
  reviewDF.createOrReplaceTempView("review")

  val splits = joinedFilterNull.randomSplit(Array(0.8, 0.2))
  val (trainingData, testData) = (splits(0), splits(1))

  def getTrainingRDD(): RDD[Rating] = {
    val trainingRdd = trainingData.rdd.map(row =>{
      val user_id_INT = row.getInt(0)
      val business_id_INT = row.getInt(1)
      val stars = row.getLong(4)
      Rating(user_id_INT.toInt, business_id_INT.toInt, stars.toDouble)
    })
    trainingRdd
  }

  def getTestingRDD(): RDD[Rating] = {
    val testingRdd = testData.rdd.map(row =>{
      val user_id_INT = row.getInt(0)
      val business_id_INT = row.getInt(1)
      val stars = row.getLong(4)
      Rating(user_id_INT.toInt, business_id_INT.toInt, stars.toDouble)
    })
    testingRdd
  }
}
