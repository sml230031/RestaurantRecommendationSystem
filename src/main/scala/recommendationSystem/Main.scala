package recommendationSystem

object Main extends App {


  val businessDataFrame = DataProcess.getBusinessDataFrame()
  val reviewDataFrame = DataProcess.getReviewDataFrame()
//  val userDataFrame = DataProcess.getUserDataFrame()

  val businessDF = businessDataFrame
  val reviewDF = reviewDataFrame

  businessDF.createOrReplaceTempView("business")
  reviewDF.createOrReplaceTempView("review")

  val splits = reviewDF.randomSplit(Array(0.8, 0.2))
  val (trainingData, testData) = (splits(0), splits(1))

  println("TrainingNum" + trainingData.count())
  println("TestingNum" + testData.count())
  println(reviewDF.count())

}
