//https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/sql/hive/SparkHiveExample.scala

import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object SparkHiveExample {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")

    sql("LOAD DATA LOCAL INPATH 'resources/kv1.txt' INTO TABLE src")

    sql("SELECT * FROM src").show()

    sql("SELECT COUNT(*) FROM src").show()

    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    val stringsDS = sqlDF.map{
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()


  }
}
