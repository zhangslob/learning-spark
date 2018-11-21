
import org.apache.avro.generic.GenericData
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkSQLExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Example")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    runProgrammaticSchemaExample(spark)

    spark.stop()
  }

  private def runProgrammaticSchemaExample(spark: SparkSession) = {
    import spark.implicits._
    // create an RDD
    val peopleRDD = spark.sparkContext.textFile("resources/people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    println("fields: " + fields)
    // fields: [Lorg.apache.spark.sql.types.StructField;@1046498a

    val schema = StructType(fields)
    println("schema: " + schema)
    // schema: StructType(StructField(name,StringType,true), StructField(age,StringType,true))

    // Convert records of the RDD (people) to Rows
    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim))

    // Apply the schema to the RDD
    val peopleDF = spark.createDataFrame(rowRDD, schema)

    // Create a temporary view using the DateFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL can be run over a temporary view created using DateFrames
    val results = spark.sql("SELECT name FROM people")

    results.map(attribute => "Name: " + attribute(0)).show()
    //+-------------+
    //|        value|
    //+-------------+
    //|Name: Michael|
    //|   Name: Andy|
    //| Name: Justin|
    //+-------------+

  }
}