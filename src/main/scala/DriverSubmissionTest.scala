//https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/DriverSubmissionTest.scala

import scala.collection.JavaConverters._
import org.apache.spark.util.Utils

//object DriverSubmissionTest {
//  def main(args: Array[String]) {
//    if (args.length < 1) {
//      println("Usage: DriverSubmissionTest <seconds-to-sleep>")
//      System.exit(1)
//    }
//    val numSecondToSleep = args(0).toInt
//
//    val env = System.getenv()
//    val properties = Utils.getSystemProperties
//
//    println("Environment variables containing SPARK_TEST:")
//    env.asScala.filter { case (k, _) => k.contains("SPARK_TEST")}.foreach(println)
//
//    println("System properties containing spark.test:")
//    properties.filter { case (k, _) => k.toString.contains("spark.test")}.foreach(println)
//
//    for (i <- 1 until numSecondToSleep) {
//      println(s"Alive for $i out of $numSecondToSleep seconds")
//      Thread.sleep(1000)
//    }
//  }
//}

