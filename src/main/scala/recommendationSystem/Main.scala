package recommendationSystem

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

  val model = ALSMatrix.run(rank,numIterations,lambda,alpha,block,seed,implicitPrefs,testingRDD)
  val topRecsForUser = model.recommendProducts(668, 6)

  println("------------------- ---------------")
  for (rating <-
         topRecsForUser) { println(rating.toString()) }
  println("------------------- ---------------")

}
