package recommendationSystem

object DataFormatTest extends App {

    val df1 = DataFormat.getBusinessData("../finalproject/yelp-dataset/yelp_academic_dataset_business.json")

    for (df <- df1) {
      println(df._2)
    }
    println("df1.size : " + df1.size)

    val df2 = DataFormat.getReviewData("../finalproject/yelp-dataset/yelp_academic_dataset_review.json")

    for(df <- df2) {
      println(df._2)
    }
  println("df2.size : " + df2.size)


  val df3 = DataFormat.getUserData("../finalproject/yelp-dataset/yelp_academic_dataset_user.json")
        for((key,value) <- df3){
          println(value)
        }
        println("df2.size : " + df3.size)
}
