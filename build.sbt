name := "RestaurantRecommendationSystem"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

val sprayGroup = "io.spray"
val sprayJsonVersion = "1.3.2"

libraryDependencies ++= List("spray-json") map {c => sprayGroup %% c % sprayJsonVersion}

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"
