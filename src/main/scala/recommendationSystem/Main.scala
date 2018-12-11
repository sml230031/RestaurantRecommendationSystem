package recommendationSystem

object Main extends App {

  import java.io.BufferedReader
  import java.io.InputStreamReader

  val br = new BufferedReader(new InputStreamReader(System.in))
  val input = br.readLine
  ModelTraining.getRecsById(input.toInt)

}
