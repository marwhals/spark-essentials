package part2_dataframes

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}
import part2_dataframes.ColumnsAndExpressions.spark
import plotly.Plotly._
import plotly._
import plotly.layout._

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
//    .config("spark.rapids.sql.enabled", "true")               // Enable RAPIDS
//    .config("spark.executor.resource.gpu.amount", "1")        // 1 GPU per executor
//    .config("spark.task.resource.gpu.amount", "0.1")          // Fractional GPU per task
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

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
  import spark.implicits._

  // 1
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))
    .show()

  // 2
  moviesDF
    .select(countDistinct(col("Director")))
    .show()

  // 3
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  // 4
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()

  /**
   * Wide transformations
   * - One or more input partitions ====> One/more output partitions
   * - See/ Make diagram. This can lead to *Shuffles*, i.e data is being move between different nodes in the spark cluster.
   * ---> This is a computationally very expensive operation
   * ---> be careful when doing data aggregations and grouping. It is best done at the end of processing.
   */

  // Compute total gross by genre
  val grossByGenre = moviesDF
    .groupBy($"Major_Genre")
    .agg(sum($"US_Gross" + $"Worldwide_Gross" + $"US_DVD_Sales").as("Total_Gross"))
    .na.drop()
    .orderBy(desc("Total_Gross"))

  // Collect to driver for visualization
  val genres = grossByGenre.select("Major_Genre").as[String].collect()
  val totals = grossByGenre.select("Total_Gross").as[Double].collect()

  // ===========================================================
  // 🎨 Plotly (Interactive) Example
  // ===========================================================
  val trace = Bar(genres.toSeq, totals.toSeq).withName("Total Gross ($)")
  val layout = Layout()
    .withTitle("Total Movie Gross by Genre (Plotly)")
    .withXaxis(Axis().withTitle("Genre"))
    .withYaxis(Axis().withTitle("Total Gross ($)"))

  plot(
    "charts/plotly-movies.html",
    Seq(trace),
    layout) // Opens interactive HTML in browser

}
