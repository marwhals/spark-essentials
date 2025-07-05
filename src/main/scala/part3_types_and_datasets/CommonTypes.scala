package part3_types_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lit, not, regexp_extract, regexp_replace}

object CommonTypes extends App {
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")


  //Adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(dramaFilter)

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie")) //Can filter on the column itself
  moviesWithGoodnessFlagsDF.where("good_movie") // where(col("good_movie") === "true")

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  // Numbers - math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // pearson correlation = number between -1(negative correlation) and 1(correlation)
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /* corr is an action - this will be performed on the data frame regardless of what ever else is in the code*/)

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalization: initcap(capitalise the first letter), lower, upper
  carsDF.select(initcap(col("Name")))

  // contains -- can miss rows
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // better way - use classic regex
  // See below for, how to
  val regexString = "volkswagen|vw" // a or b
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract") //Drop this column since it is not useful

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /**
   * TODO: Exercise
   *
   * Filter the cars DF by a list of car names obtained by an API call
   * Versions:
   *   - contains
   *   - regexes
   */



}
