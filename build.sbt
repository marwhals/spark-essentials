name := "spark-essentials"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"
val postgresVersion = "42.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,

  // PostgreSQL JDBC driver
  "org.postgresql" % "postgresql" % postgresVersion,

  // Optional logging
  "org.apache.logging.log4j" % "log4j-api"  % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0"
)

// Use Java 21 features
javacOptions ++= Seq("--release", "21")
scalacOptions ++= Seq("-target:jvm-21")

// Run Spark correctly in forked JVM
fork in run := true