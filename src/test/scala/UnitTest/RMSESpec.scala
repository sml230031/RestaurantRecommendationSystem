package UnitTest

import org.scalatest.{FlatSpec, Matchers}
import recommendationSystem.ModelTraining

class RMSESpec extends FlatSpec with Matchers {

  behavior of "compute RMSE and evaluation"

  it should "get a good rmse" in {
    val rmse = ModelTraining.computeRmse()
    assert(rmse < 2)
  }

}
