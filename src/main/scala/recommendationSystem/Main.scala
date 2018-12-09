package recommendationSystem

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Main extends App {
  val rank = 20
  val numIterations = 15
  val lambda = 0.10
  val alpha = 1.00
  val block = -1
  val seed = 12345L
  val implicitPrefs = false

  val trainingRDD = RDD.getTrainingRDD()
  val testingRDD = RDD.getTestingRDD()

//  val spark = SparkSession
//    .builder
//    .appName("model")
//    .master("local[2]")
//    .getOrCreate()

  val model = ALSMatrix.run(rank,numIterations,lambda,alpha,block,seed,implicitPrefs,testingRDD)
//  val savedALSModel = ALSMatrix.run(rank,numIterations,lambda,alpha,block,seed,implicitPrefs,testingRDD).save(spark.sparkContext, "model/MovieRecomModel")
//  val model = MatrixFactorizationModel.load(spark.sparkContext,"model/MovieRecomModel/")
  val topRecsForUser = model.recommendProducts(1, 6)

  println("------------------- ---------------")
  for (rating <-
         topRecsForUser) { println(rating.toString()) }
  println("------------------- ---------------")


}
