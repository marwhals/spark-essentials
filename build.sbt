name := "spark-essentials"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion     = "3.5.1"
val postgresVersion  = "42.6.0"
val plotlyScalaVersion = "0.8.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql"  % sparkVersion,

  "org.postgresql" % "postgresql" % postgresVersion,

  "org.apache.logging.log4j" % "log4j-api"  % "2.20.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.20.0",

  "org.plotly-scala" %% "plotly-almond" % plotlyScalaVersion   // Interactive Plotly charts
)

// --- Java 11 compatibility ---
javacOptions ++= Seq("-source", "11", "-target", "11")
scalacOptions += "-target:jvm-11"

// --- Run Spark correctly in forked JVM ---
fork / run := true
