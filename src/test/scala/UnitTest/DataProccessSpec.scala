package UnitTest

import org.scalatest.{FlatSpec, Matchers}
import recommendationSystem.DataProcess

class DataProccessSpec extends FlatSpec with Matchers {

  behavior of "Get correct DataFrame"

  it should "get correct business dataframe" in {
    val businessDF = DataProcess.getBusinessDataFrame()
    val size = businessDF.count()
    size shouldBe 72665
  }

  it should "get correct review dataframe" in {
    val reviewDF = DataProcess.getReviewDataFrame()
    val size = reviewDF.count()
    size shouldBe 3855973
  }

  it should "get correct user dataframe" in {
    val userDF = DataProcess.getUserDataFrame()
    val size = userDF.count()
    size shouldBe 1518169
  }


}
