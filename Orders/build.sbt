name := "Orders"

version := "3.0.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.5.0" % "provided",
  "mysql" % "mysql-connector-java" % "5.1.37",
  "com.amazon.redshift" % "jdbc4" % "1.1.10.1010" from "https://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.10.1010.jar"
)
