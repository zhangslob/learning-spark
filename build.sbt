name := "learning-spark"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "org.apache.spark" %% "spark-hive" % "2.1.0"
)
