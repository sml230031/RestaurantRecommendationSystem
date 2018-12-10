package recommendationSystem

import java.io.{File, PrintWriter}

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ModelTrainning extends App{

  val trainingRDD = RDD.getTrainingRDD().cache()
  val testingRDD = RDD.getTestingRDD().cache()

  val rank = 30
  val iterations = 15
  val lambda = 0.8

  val spark = SparkSession
    .builder
    .appName("model")
    .master("local[2]")
    .getOrCreate()

  //  def train(ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double)

  val writer = new PrintWriter(new File("model/rank.txt" ))
//  for(rank <- 20 to 100; if(rank % 10 == 0)) {
    val model = ALS.train(trainingRDD, rank, iterations, lambda)
    model.save(spark.sparkContext, "model/MovieRecomModel")
    val rmseTest = computeRmse(model, testingRDD, true)
    writer.println("Test RMSE: = " + rmseTest + " , rank = " + rank + "\n")

//  }
  writer.close()

  //  val model = ALS.train(trainingRDD, rank, iterations, lambda)
//  model.save(spark.sparkContext, "model/MovieRecomModel")
//  val rmseTest = computeRmse(model, testingRDD, true)
//  println("Test RMSE: = " + rmseTest)

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean)
  : Double = {

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), (x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }


}
