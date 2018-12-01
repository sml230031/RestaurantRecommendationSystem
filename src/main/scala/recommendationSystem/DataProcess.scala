package recommendationSystem

import org.apache.spark.sql.functions.{lit, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataProcess {

  def getBusinessDataFrame(): DataFrame = {
    val spark = SparkSession
      .builder
      .appName("businessDataFrame")
      .master("local[2]")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_business.json").withColumn("business_id_INT",monotonicallyIncreasingId)
    df.select("business_id", "name", "state", "city", "address","business_id_INT")
  }

  def getReviewDataFrame(): DataFrame = {
    val spark = SparkSession
      .builder
      .appName("reviewDataFrame")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_review.json")
    df.select("business_id", "user_id", "stars", "date")
      .filter(to_date(df("date"),"yyyy-MM-dd").gt(lit("2015-01-01")))
  }

  def getUserDataFrame(): DataFrame = {
    val spark = SparkSession
      .builder
      .appName("userDataFrame")
      .master("local[2]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_user.json").withColumn("user_id_INT",monotonicallyIncreasingId)
    df
  }

//  def getReady(): Unit = {
//    val businessDataFrame = getBusinessDataFrame()
//    val reviewDataFrame = getReviewDataFrame()
//    //  val userDataFrame = DataProcess.getUserDataFrame()
//
//    val businessDF = businessDataFrame
//    val reviewDF = reviewDataFrame
//
//    businessDF.createOrReplaceTempView("business")
//    reviewDF.createOrReplaceTempView("review")
//
//    val splits = reviewDF.randomSplit(Array(0.8, 0.2))
//    val (trainingData, testData) = (splits(0), splits(1))
//
//    val reviewRdd = trainingData.rdd.map(row =>{
//      val businessID = row.getString(0)
//      val userID = row.getString(1)
//      val stars = row.getString(2)
//      Rating(businessID, userID, stars.toFloat)
//    }
//    )
//
//    val testRdd = testData.rdd.map(row =>{
//      val businessID = row.getString(0)
//      val userID = row.getString(1)
//      val stars = row.getString(2)
//      Rating(businessID,userID,stars.toFloat)
//    }
//    )
//  }
// test
}

