package recommendationSystem

import java.io.FileNotFoundException

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.SparkSession

object ModelTraining {

  val trainingRDD = RDD.getTrainingRDD()
  val testingRDD = RDD.getTestingRDD()

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
      case ex: FileNotFoundException => {
        println("Model Training Start")
        ModelTraining.training
      }
    }
  }

  def getRecsById(userID: Int) : Unit = {
      val model = ModelTraining.getModel
      val topRecsForUser = model.recommendProducts(userID, 150000)
    println("------------------- ---------------")
    for (rating <-
           topRecsForUser) { println(rating.toString()) }
    println("------------------- ---------------")
    val rmseTest = RMSE.computeRmse(model, testingRDD, true)
    println("Test RMSE: = " + rmseTest) //Less is better
  }

}
