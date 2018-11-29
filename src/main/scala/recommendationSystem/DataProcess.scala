package recommendationSystem

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataProcess {

  def getBusinessDataFrame(): DataFrame = {
    val spark = SparkSession
      .builder
      .appName("businessDataFrame")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_business.json")
    df
  }

  def getReviewDataFrame(): DataFrame = {
    val spark = SparkSession
      .builder
      .appName("reviewDataFrame")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_review.json")
    df
  }

  def getUserDataFrame(): DataFrame = {
    val spark = SparkSession
      .builder
      .appName("reviewDataFrame")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_user.json")
    df
  }

}

