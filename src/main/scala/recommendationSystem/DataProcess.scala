package recommendationSystem

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.lit

object DataProcess {

  def getBusinessDataFrame(): DataFrame = {
    val spark = SparkSession
      .builder
      .appName("businessDataFrame")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_business.json")
    df.select("business_id", "name", "state", "city", "address")
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

    val df = spark.read.json("../finalproject/yelp-dataset/yelp_academic_dataset_user.json")
    df
  }

}

