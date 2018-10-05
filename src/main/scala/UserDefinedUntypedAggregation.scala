////Untyped User-Defined Aggregate Functions
//
//import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.OL
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.expressions.MutableAggregationBuffer
//import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
//import org.apache.spark.sql.types._
//
//
//object UserDefinedUntypedAggregation {
//  object MyAverage extends UserDefinedAggregateFunction {
//    def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)
//
//    def bufferSchema: StructType = {
//      StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
//    }
//
//    def dataType: DataType = DoubleType
//
//    def deterministic: Boolean = true
//
//    def initialize(buffer: MutableAggregationBuffer): Unit = {
//      buffer(0) = OL
//      buffer(1) = OL
//    }
//
//    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
//      if (!input.isNullAt(0)) {
//        buffer(0) = buffer.getLong(0) + input.getLong(0)
//        buffer(1) = buffer.getLong(1) + 1
//      }
//    }
//
//    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
//      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
//    }
//
//    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
//
//    def main(args: Array[String]): Unit = {
//      val spark = SparkSession
//        .builder()
//        .appName("Spark SQL user-defined DataFrames aggregation example")
//        .config("spark.master", "local")
//        .getOrCreate()
//
//      spark.sparkContext.setLogLevel("ERROR")
//      spark.udf.register("myAverage", MyAverage)
//      val df = spark.read.json("employees.json")
//      df.createOrReplaceTempView("employees")
//      df.show()
//
//      val result = spark.sql("SELECT myAverage(salary as average_salary FROM employees")
//      result.show()
//
//    }
//
//  }
//}
//
//
