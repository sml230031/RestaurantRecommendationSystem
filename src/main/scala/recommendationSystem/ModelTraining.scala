package recommendationSystem

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.annotation.tailrec

object ModelTraining {

  var rdd = RDD
  val trainingRDD = rdd.getTrainingRDD()
  val testingRDD = rdd.getTestingRDD()
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
        println("------------------- ---------------")


    val containedBusiness_Id = for (rating <- topRecsForUser) yield (rating.product, rating.rating) // select the business_Id related to UserID


    println("Filter Start")

    val filteredBusiness_id = businessDF.where(businessDF("city").equalTo(city)).select("business_id_INT").rdd.map(r => r(0)).collect()
    // select business_id that related to Location

    val filteredBusinessDF = businessDF.where(businessDF("city").equalTo(city)).unpersist()
    // select business

    val validRating  = for (f <- filteredBusiness_id; c <- containedBusiness_Id; if c._1.equals(f)) yield c
    //select selected business's Rating data from TopRec

    import spark.sqlContext.implicits._
    val ratingColunm = validRating.toSeq.toDF("business_id_INT", "Rating")

    println(filteredBusiness_id.length)
    println(filteredBusinessDF.count())
    println( ratingColunm.count())

    val BwithRatingDF = filteredBusinessDF.join(ratingColunm,Seq("business_id_INT"),"left_outer").na.drop()

    BwithRatingDF.orderBy(BwithRatingDF("Rating").desc).select("name","state","city","address","Rating").show(false)

    print ("Filter End")

  }

  def computeRmse(): Double = {
    val model = getModel
    val data = rdd.getTestingRDD()
    val implicitPrefs = false
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x => ((x.user, x.product), x.rating) }
      .join(data.map(x => ((x.user, x.product), x.rating))).values
    if (implicitPrefs) { println("(Prediction, Rating)")
      println(predictionsAndRatings.take(5).mkString("n")) }
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 -
      x._2)).mean())
  }


}
