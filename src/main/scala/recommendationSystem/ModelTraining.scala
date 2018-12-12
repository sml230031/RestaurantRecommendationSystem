package recommendationSystem

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.{DataFrame, SparkSession}


import scala.annotation.tailrec

object ModelTraining {

  val trainingRDD = RDD.getTrainingRDD()
  val testingRDD = RDD.getTestingRDD()
  val businessDF = DataProcess.getBusinessDataFrame().persist()

  val spark = SparkSession
    .builder
    .appName("model")
    .master("local[2]")
    .getOrCreate()

  def training : MatrixFactorizationModel  = {
    val rank = 20
    val numIterations = 15
    val lambda = 0.75
    val alpha = 1.00
    val block = -1
    val seed = 12345L
    val implicitPrefs = false

    val model = ALSMatrix.run(rank,numIterations,lambda,alpha,block,seed,implicitPrefs,trainingRDD)
    val savedALSModel = ALSMatrix.run(rank,numIterations,lambda,alpha,block,seed,implicitPrefs,trainingRDD).save(spark.sparkContext, "model/MovieRecomModel")
    model
  }

  def getModel : MatrixFactorizationModel = {
    try {
      val model = MatrixFactorizationModel.load(spark.sparkContext, "model/MovieRecomModel/")
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




  def getRecsById(userID: Int) : Unit = {
      val model = ModelTraining.getModel
      val topRecsForUser = model.recommendProducts(userID, 1500)
    for (rating <- topRecsForUser) { println(rating.toString()) }
        println("------------------- ---------------")

      val containedBusiness_Id = for (rating <- topRecsForUser) yield (rating.product, rating.rating) // select the business_Id related to UserID
      println("Filter Start")

      import org.apache.spark.sql.functions
      val business = for (b <- containedBusiness_Id) yield businessDF.where(businessDF("business_id_INT").equalTo(b._1)).withColumn("Rating", functions.lit(b._2))

      val result = foldLeft(business.toList)// Union all business datasets

     result.show(1500)

  }

}
