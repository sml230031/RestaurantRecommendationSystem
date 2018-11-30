package recommendationSystem

import org.apache.spark.mllib.recommendation.ALS

case class ALSMatrixCons(rank: Int, numIterations: Int, lambda: Double, alpha: Double,
                         block: Int, seed: Long, implicitPrefers: Boolean, model: ALS)

object ALSMatrixCons {

  def apply(rank: Int, numIterations: Int, lambda: Double, alpha: Double,
            block: Int, seed: Long, implicitPrefers: Boolean, model: ALS) = {

    //rank: Int, numIterations: Int, lambda: Double, alpha: Double,
    //block: Int, seed: Long, implicitPrefers: Boolean, model: ALS
    val model = new ALS().setIterations(numIterations).setBlocks(block).setAlpha(alpha).setLambda(lambda)

  }


}
