//https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/HdfsTest.scala

import org.apache.spark.sql.SparkSession

object HdfsTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: HdfsTest <file>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("HdfsTest")
      .getOrCreate()

    val file = spark.read.text(args(0)).rdd
    val mapped = file.map(s => s.length).cache()

    for (iter <- 1 to 10) {
      val start = System.currentTimeMillis()
      for (x <- mapped) { x + 2}
      val end = System.currentTimeMillis()
      println(s"Iteration $iter took ${end-start} ms")
    }

    spark.stop()
  }
}
