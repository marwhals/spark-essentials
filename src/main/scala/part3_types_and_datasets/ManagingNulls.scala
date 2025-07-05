package part3_types_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col}

/**
 * Nullables
 * - Non-nullable
 *  - are NOT a constraint
 *  - are a marker for Spark to optimise for nulls
 *  - can lead to expectations or data errors if broken
 */

object ManagingNulls extends App {

  val spark = SparkSession.builder()
    .appName("Managing Nulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //------------------------------------------------------------------------

  // Select the first non-nullable value from these columns
  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  )

  // Checking for nulls
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)
  // Nulls when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)
  // Removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // Remove any rows containing nulls.
  // Replace nulls
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0, //Default value when spark finds null
    "Rotten_Tomatoes_Rating" -> 10, //Default value when spark finds null
    "Director" -> "Unknown" //Default value when spark finds null
  ))
  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif", // returns null if the two values are EQUAL, else first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show()

}
