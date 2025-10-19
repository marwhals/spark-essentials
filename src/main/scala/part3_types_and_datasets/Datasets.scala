package part3_types_and_datasets

import org.apache.spark.sql.functions.{array_contains, avg, col}

import java.sql.Date
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import part3_types_and_datasets.CommonTypes.spark

/**
 * Datasets
 * - Type DataFrames: Distributed collection of JVM objects
 *
 * Most useful when
 * - We want to maintain type information
 * - We want clean concise code
 * - Our filters / transformations are hard to express in DataFrames methods and functions or SQL
 *
 * Avoid when
 * - Performance is critical since Spark will not be able to optimise transformations
 *  - All the transformations and filters are plain Scala objects that will be evaluated at run time
 *  - That is after Spark has had a change to optimise all the operations in advance.
 *    - Spark will have to evaluate all the filters and transformations on a row-by-row basis which is very slow
 *
 * Tradeoffs:
 * - TypeSafety --> then use DataSets
 * - Fast performance --> then use DataFrames
 *
 * Subtle Note: DataFrame = Dataset[Row] i.e same thing but using the "Row" type
 */

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert a DF to a Dataset -- Spark allows us to add more information to the columns of a DataFrame
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type
  // 1 - define your case class -- field names need to have the same names as what is in the JSON
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double], // Use options to allow for Nulls in a data set
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._
  // 4 - convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // Now have access to map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
   * Exercises:
   * 1. Count how many cars we have
   * 2. Count how many powerful cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

  // 1
  val carsCount = carsDS.count
  println(carsCount)

  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _)/ carsCount)

  // alternative
  carsDS.select(avg(col("Horsepower"))).show

  /**
   * Joins
   */
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars_data/guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitars_data/guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("guitars_data/bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner") // Can rename the columns using ".withColumnRenamed"
  guitarPlayersDS.show

  /**
   * Exercise: Join the guitarDS and guitarPlayersDS in an outer join (hint: use array_contains)
   */

  guitarPlayersDS
    .joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
    .show()

  // Grouping DS

  val carsGroupedByOrigin = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are wide transformations -> they can change the number of partitions that will back those data sets. This leads to shuffle operations


}
