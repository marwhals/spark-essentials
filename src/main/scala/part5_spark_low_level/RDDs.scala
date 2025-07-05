package part5_spark_low_level

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
 * RDDs - Resilient Distributed Datasets
 * Important:
 * - Read from external sources
 * - Convert to/from DataFrames and Datasets
 * - Difference between RDDs, DataFrames and Datasets
 *
 *
 * RDDs
 * - Distributed typed collections of JVM objects
 * - The "first citizens" of Spark: all higher-level APIs reduce to RDDs
 * - Pros: Can be highly optimised
 *  - Partitioning can be controlled
 *  - Order of elements can be controlled
 *  - Order of operations matters for performance!!!!!!
 * - Cons: hard to work with
 *  - for complext operations, need to know the internals of Spark
 *  - poor APIs for quick data processing
 *
 *  *** For 99% of operations, use the DataFrame/Dataset APIs
 */

object RDDs extends App {

  val spark = SparkSession.builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  // the SparkContext is the entry point for low-level APIs, including RDDs
  val sc = spark.sparkContext

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

  /**
   * RDDs vs DataFrames/Datasets
   *
   * In common
   * - Collection API: map, flatMap, filter, take, reduce etc
   * - union, count, distinct
   * - groupBy, sortBy
   *
   * RDDs over Datasets - i.e what can RDDs do
   * - Partition control: repartition, coalesce, paritioner, zipPartitions, mapPartitions
   * - Operation control: checkpoint, isCheckpointed, localCheckpoint, cache
   * - Storage Control: cache, getStorageLevel, persist
   *
   * Datasets over RDDs
   * - Can perform select and join
   * - Spark can plan/optimise before running code
   *
   * For 99% of operations, use the DataFrame/ Dataset APIs
   *
   */

}
