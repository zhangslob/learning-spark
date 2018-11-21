import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;

import static org.apache.spark.sql.functions.col;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


public class JavaSparkSQLExample {
    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL Example")
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        // runBasicDateFrameExample(spark);
        // runDatasetCreationExample(spark);
        // runInferSchemaExample(spark);
        runProgrammaticSchemaExample(spark);
        spark.stop();
    }

    private static void runBasicDateFrameExample(SparkSession spark) throws AnalysisException {
        Dataset<Row> df = spark.read().json("resources/people.json");

        df.show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+

        df.printSchema();
        //root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        df.select(col("name"), col("age").plus(1)).show();
        //+-------+---------+
        //|   name|(age + 1)|
        //+-------+---------+
        //|Michael|     null|
        //|   Andy|       31|
        //| Justin|       20|
        //+-------+---------+

        df.filter(col("age").gt(21)).show();
        //+---+----+
        //|age|name|
        //+---+----+
        //| 30|Andy|
        //+---+----+

        df.groupBy("age").count().show();
        //+----+-----+
        //| age|count|
        //+----+-----+
        //|  19|    1|
        //|null|    1|
        //|  30|    1|
        //+----+-----+

        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+

        df.createGlobalTempView("people");
        spark.sql("SELECT * FROM global_temp.people").show();

        spark.newSession().sql("SELECT * FROM global_temp.people").show();
    }

    private static void runDatasetCreationExample(SparkSession spark) {
        Person person = new Person();
        person.setName("Andy");
        person.setAge(21);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();
        //+---+----+
        //|age|name|
        //+---+----+
        //| 21|Andy|
        //+---+----+

//        Encoder<Integer> integerEncoder = Encoders.INT();
//        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
//        Dataset<Integer> transformedDS = primitiveDS.map(
//                (MapFunction<Integer, Integer>) value -> value + 1,
//                integerEncoder
//        );
//        transformedDS.collect();

        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect();


        String path = "resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
        //+----+-------+
        //| age|   name|
        //+----+-------+
        //|null|Michael|
        //|  30|   Andy|
        //|  19| Justin|
        //+----+-------+
    }

    private static void runInferSchemaExample(SparkSession spark) {
        JavaRDD<Person> peopleRDD = spark.read()
                .textFile("resources/people.txt")
                .javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Person person = new Person();
                    person.setName(parts[0]);
                    person.setAge(Integer.parseInt(parts[1].trim()));
                    return person;
                });

        Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);
        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                stringEncoder
        );
        teenagerNamesByIndexDF.show();

        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getAs("name"),
                stringEncoder
        );
        teenagerNamesByFieldDF.show();
    }

    private static void runProgrammaticSchemaExample(SparkSession spark) {
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("resources/people.txt", 1)
                .toJavaRDD();

        String schemaString = "name age";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName: schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attributes = record.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim());
        });

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

        peopleDataFrame.createOrReplaceTempView("people");

        Dataset<Row> results = spark.sql("SELECT name FROM people");

        Dataset<String> namesDS = results.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                Encoders.STRING()
        );
        namesDS.show();
    }
}
