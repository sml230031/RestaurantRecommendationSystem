package recommendationSystem

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

object ALSMatrix {
  def run(rank: Int, numIterations: Int, lambda: Double, alpha: Double,
          block: Int, seed: Long, implicitPrefers: Boolean, trainingRdd: RDD[Rating]): MatrixFactorizationModel = {

    //rank: Int, numIterations: Int, lambda: Double, alpha: Double,
    //block: Int, seed: Long, implicitPrefers: Boolean, model: ALS
    val model = new ALS().setIterations(numIterations)
      .setBlocks(block)
      .setAlpha(alpha)
      .setLambda(lambda)
      .setRank(rank)
      .setSeed(seed)
      .setImplicitPrefs(implicitPrefers)
      .run(trainingRdd)
    model
  }


}
