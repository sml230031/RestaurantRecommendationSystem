package recommendationSystem

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object RMSE {
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean): Double = { val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x => ((x.user, x.product), x.rating) }
      .join(data.map(x => ((x.user, x.product), x.rating))).values
    if (implicitPrefs) { println("(Prediction, Rating)")
      println(predictionsAndRatings.take(5).mkString("n")) }
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 -
      x._2)).mean())
  }

}
