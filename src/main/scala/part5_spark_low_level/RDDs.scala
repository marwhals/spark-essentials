package part5_spark_low_level

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import part2_dataframes.Aggregations.spark

import scala.io.Source

/**
 * RDDs - Resilient Distributed Datasets
 * Important:
 * - Read from external sources
 * - Convert to/from DataFrames and Datasets
 * - Difference between RDDs, DataFrames and Datasets
 *
 * RDDs
 * - Distributed typed collections of JVM objects
 * - The "first citizens" of Spark: all higher-level APIs reduce to RDDs
 * - Pros: Can be highly optimised
 *  - Partitioning can be controlled
 *  - Order of elements can be controlled
 *  - Order of operations matters for performance!!!!!!
 * - Cons: hard to work with
 *  - for complex operations, need to know the internals of Spark
 *  - poor APIs for quick data processing
 *
 *
 * RDDs vs DataFrames/Datasets
 * ---------
 * In common
 * - Collection API: map, flatMap, filter, take, reduce etc
 * - union, count, distinct
 * - groupBy, sortBy
 * -----------
 * RDDs over Datasets - i.e what can RDDs do
 * - Partition control: repartition, coalesce, paritioner, zipPartitions, mapPartitions
 * - Operation control: checkpoint, isCheckpointed, localCheckpoint, cache
 * - Storage Control: cache, getStorageLevel, persist
 * ------------
 * Datasets over RDDs
 * - Can perform select and join
 * - Spark can plan / optimise before running code
 *
 * --------> For 99% of operations, use the DataFrame / Dataset APIs
 *
 */

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local[*]")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

  spark.sparkContext.setLogLevel("ERROR")

  // 1 - parallelize an existing collection
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers) // Turn a regular collection(numbers is just an example) into an RDD

  // 2 - reading from files
  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) = {
    val source = Source.fromFile(filename)
    val stockValues = source.getLines()
      .drop(1)
      .map(line => line.split(","))
      .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
      .toList
    source.close()
    stockValues
  }

  val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  // 2b - reading from files
  val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv") // Read RDD from file
    .map(line => line.split(",")) // need to process each line in turn
    .filter(tokens => tokens(0).toUpperCase() == tokens(0))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

  // 3 - read from a DF
  val stocksDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/stocks.csv")

  import spark.implicits._
  val stocksDS = stocksDF.as[StockValue]
  val stocksRDD3 = stocksDS.rdd //Dataset to RDD - all Datasets can access underlying RDDs

  // RDD -> DF ---- RDDs to high-level API
  val numbersDF = numbersRDD.toDF("numbers") // Type information is lost

  // RDD -> DS ---- RDDs to high-level API
  val numbersDS = spark.createDataset(numbersRDD) // Type information is kept

  // Transformations

  // distinct
  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // <---- lazy transformation
  val msCount = msftRDD.count() // <---- eager action

  // counting
  val companyNamesRDD = stocksRDD.map(_.symbol).distinct() //  district is also lazy

  // min and max
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((sa: StockValue, sb: StockValue) => sa.price < sb.price)
  val minMsft = msftRDD.min() // action

  // reduce
  numbersRDD.reduce(_ + _)

  // grouping
  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
  // ^^ very expensive

  // Partitioning

  val repartitionedStocksRDD = stocksRDD.repartition(30)
  repartitionedStocksRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")
  /**
    Repartitioning is expensive. Involves Shuffling.
    Best practice: partition early and then process .
    Optimal size of a partition is 10-100MB.
   */

  // coalesce - will repartition and RDD to fewer than it already has
  val coalescedRDD = repartitionedStocksRDD.coalesce(15) // does NOT involve shuffling. Data is shuffled to the RDD
  coalescedRDD.toDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks15")

  /**
   * Exercises
   * 1) Read the movies.json as an RDD.
   * 2) Show the distinct genres as an RDD.
   * 3) Select all the movies in the Drama genre with IMDB rating > 6.
   * 4) Show the average rating of movies by genre.
   *
   */

  case class Movie(title: String, genre: String, rating: Double)

  // - Read movies as an RDD
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesRDD = moviesDF
    .select(col("Title").as("title"), col("Major_Genre").as("genre"), col("IMDB_Rating").as("rating"))
    .where(col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  // 2
  val genresRDD = moviesRDD.map(_.genre).distinct()

  // 3
  val goodDramasRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

  moviesRDD.toDF.show()
  genresRDD.toDF().show()
  goodDramasRDD.toDF().show()

  // 4
  case class GenreAvgRating(genre: String, rating: Double)

  val avgRatingByGenreRDD: RDD[GenreAvgRating] = moviesRDD.groupBy(_.genre).map {
    case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
  }

  avgRatingByGenreRDD.toDF.show

  moviesRDD.toDF.groupBy(col("genre")).avg("rating").show



}
