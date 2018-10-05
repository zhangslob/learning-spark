
//https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala

import java.util.Properties
import org.apache.spark.sql.SparkSession

object SQLDataSourceExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL dara sources example")
      .config("name", "value")
      .getOrCreate()

    // do something
    // runBacisDataSourceExample(spark)
    // runBasicParquetExample(spark)
    // runParquetSchemaMergingExample(spark)
    // runJsonDatesetExample(spark)
    runJdbcDatasetExample(spark)

    spark.stop()
  }

  private def runBacisDataSourceExample(spark: SparkSession): Unit = {
    val usersDF = spark.read.load("resources/users.parquet")
    usersDF.select("name", "favorite_color").write.save("nameAndFavColors.parquet")

    val peopleDF = spark.read.format("json").load("resources/people.json")
    peopleDF.select("name", "age").write.save("namesAndAges.parquet")

    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("resources/people.csv")

    // $example on: direct_sql
    val sqlDF = spark.sql("SELECT * FROM parquet.`resources/users.parquet`")

    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    usersDF.write.partitionBy("favorite_color").format("parquet").save("namePartByColor.parquet")
    usersDF
      .write
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")
  }

  private def runBasicParquetExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleDF = spark.read.json("resources/people.json")
    peopleDF.write.parquet("people.parquet")

    val parquetFileDF = spark.read.parquet("people.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")

    val namesDF = spark.sql(
      """
        |SELECT name
        |FROM parquetFile
        |WHERE age BETWEEN 13 AND 19
      """.stripMargin)

    namesDF.map(attributes => "Name: " + attributes(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
    // $example off:basic_parquet_example$
  }

  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val squaresSD = spark.sparkContext.makeRDD(1 to 5)
      .map(i => (i, i * i))
      .toDF("value", "square")
    squaresSD.write.parquet("data/test_table/key=1")

    val cubesDF = spark.sparkContext.makeRDD(6 to 10)
      .map(i => (i, i * i * i))
      .toDF("value", "cube")
    cubesDF.write.parquet("data/test_table/key=2")

    val mergeDF = spark.read.option("mergeSchema", "true")
      .parquet("data/test_table")
    mergeDF.show()
    mergeDF.printSchema()
    // +-----+------+----+---+
    // |value|square|cube|key|
    // +-----+------+----+---+
    // |    1|     1|null|  1|
    // |    2|     4|null|  1|
    // |    3|     9|null|  1|
    // |    4|    16|null|  1|
    // |    5|    25|null|  1|
    // |    6|  null| 216|  2|
    // |    7|  null| 343|  2|
    // |    8|  null| 512|  2|
    // |    9|  null| 729|  2|
    // |   10|  null|1000|  2|
    // +-----+------+----+---+

    // root
    //  |-- value: integer (nullable = true)
    //  |-- square: integer (nullable = true)
    //  |-- cube: integer (nullable = true)
    //  |-- key: integer (nullable = true)
  }

  private def runJsonDatesetExample(spark: SparkSession): Unit = {
    import spark.implicits._

    val path = "resources/people.json"
    val peopleDF = spark.read.json(path)

    peopleDF.printSchema()

    peopleDF.createOrReplaceTempView("people")

    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()

//    val otherPeopleDataset = spark.createDataset(
//      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
//    val otherPeople = spark.read.json(otherPeopleDataset)
//    otherPeople.show()
  }

  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")

    val jdbcDF2 = spark.read.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    connectionProperties.put("customerSchema", "id DECIMAL(38, 0), name STRING")

    val jdbcDF3 = spark.read.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()
  }
}







