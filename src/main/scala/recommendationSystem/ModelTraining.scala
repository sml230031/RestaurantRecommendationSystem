package recommendationSystem

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

object ModelTraining {

  val trainingRDD = RDD.getTrainingRDD()
  val testingRDD = RDD.getTestingRDD()
  val businessDF = DataProcess.getBusinessDataFrame()

  val spark = SparkSession
    .builder
    .appName("model")
    .master("local[2]")
    .getOrCreate()

  def training : MatrixFactorizationModel  = {
    val rank = 20
    val numIterations = 15
    val lambda = 0.4
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = false

    val model = ALSMatrix.run(rank,numIterations,lambda,alpha,block,seed,implicitPrefs,trainingRDD)
    val savedALSModel = ALSMatrix.run(rank,numIterations,lambda,alpha,block,seed,implicitPrefs,trainingRDD).save(spark.sparkContext, "model/RestaurantRecomModel")
    model
  }

  def getModel : MatrixFactorizationModel = {
    try {
      val model = MatrixFactorizationModel.load(spark.sparkContext, "model/RestaurantRecomModel/")
      model
    } catch {
      case ex: InvalidInputException=> {
        println("Model Training Start")
        ModelTraining.training
      }
    }
  }

  def foldLeft(xs: List[DataFrame]): DataFrame = {
    @tailrec
    def inner( result: DataFrame, work: List[DataFrame] ): DataFrame = work match {
      case Nil => result
      case h :: t => inner(result.union(h) ,t)
    }
    inner(xs.head,xs)
  }




  def getRecsById(userID: Int, city: String) : Unit = {
    val model = ModelTraining.getModel
    val topRecsForUser = model.recommendProducts(userID, 150000)
    for (rating <- topRecsForUser) { println(rating.toString()) }
    println(topRecsForUser.length)
        println("------------------- ---------------")


    val containedBusiness_Id = for (rating <- topRecsForUser) yield (rating.product, rating.rating) // select the business_Id related to UserID


    println("Filter Start")

    val filteredBusiness_id = businessDF.where(businessDF("city").equalTo(city)).select("business_id_INT").rdd.map(r => r(0)).collect()



    val filteredBusinessDF = businessDF.where(businessDF("city").equalTo(city)).unpersist()

    val validRating  = for (f <- filteredBusiness_id; c <- containedBusiness_Id; if c._1.equals(f)) yield c

    import spark.sqlContext.implicits._
    val ratingColunm = validRating.toSeq.toDF("business_id_INT", "Rating")

    println(filteredBusiness_id.length)
    println(filteredBusinessDF.count())
    println( ratingColunm.count())

    val BwithRatingDF = filteredBusinessDF.join(ratingColunm,Seq("business_id_INT"),"left_outer").na.drop()

    BwithRatingDF.orderBy(BwithRatingDF("Rating").desc).show(false)

//    validRating.foreach{ println(_)}

//    val result = filteredBusinessDF.withColumn("rating",validRating)

    print ("Filter End")



//    var i : Int = 0
//    val list = List.apply()


//    def getValidID : List[(Int,Double)] = {
//      for (b <- containedBusiness_Id) {
//        if (filteredBusinessDF.where(filteredBusinessDF("business_id_INT").equalTo(b._1)).isEmpty && i < 5) {
//          i = i + 1
//          list.+:(b)
//        }
//      }
//      list
//    }
//    val validID = for (b <- containedBusiness_Id ; if filteredBusinessDF.filter(filteredBusinessDF("bussiness_id_INT").
//    println("get valid ID")
//    validID.foreach(println(_))
//
//    import org.apache.spark.sql.functions
//
//    val business = for (b <- validRating)
//      yield filteredBusinessDF.where(filteredBusinessDf.withColumn("Rating", functions.lit(b._2))
//
//    val result = foldLeft(business.toList)// Union all business datasets
//
//     result.show()

  }

}
