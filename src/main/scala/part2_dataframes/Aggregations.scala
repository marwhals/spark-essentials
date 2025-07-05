package part2_dataframes

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, stddev}

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  //------------------------

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*")) // count all the rows, and will INCLUDE nulls
  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre"))).show()
  // approximate count --- won't scan a data frame row by row. Will give you an approximate row count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))
  // min and max
  val minRatingDF = moviesDF.select(functions.min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")
  // sum
  moviesDF.select(functions.sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")
  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")
  // other stats --- TODO consider making my own
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping
  val countByGenreDF = moviesDF
    .groupBy(col("Major_Genre")) // Includes NULL
    .count()  // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  /**
   * TODO - Exercises
   * 1) - Sum up all the profits of all the movies in the DF
   * 2) - Count how many distinct directors we have
   * 3) - Show the mean and standard deviation of US gross revenue for the movies
   * 4) - Compute the average IMDB rating and the average US gross revenue per director.
   */

  /**
   * Wide transformations
   * - One or more input partitions ====> One/more output partitions
   * - See/ Make diagram. This can lead to *Shuffles*, i.e data is being move between different nodes in the spark cluster.
   * ---> This is a computationally very expensive operation
   * ---> be careful when doing data aggregations and grouping. It is best done at the end of processing.
   */

}
