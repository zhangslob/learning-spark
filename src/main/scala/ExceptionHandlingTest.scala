//https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/ExceptionHandlingTest.scala

import org.apache.spark.sql.SparkSession

object ExceptionHandlingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ExceptionHandlingTest")
      .getOrCreate()

    spark.sparkContext.parallelize(0 until spark.sparkContext.defaultParallelism).foreach {
      i =>
        if (math.random > 0.75) {
          println("Existing =======")
          throw new Exception("Testing exception handling")
        }
    }

    spark.stop()
  }
}
