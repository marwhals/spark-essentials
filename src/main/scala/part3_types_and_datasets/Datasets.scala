package part3_types_and_datasets

import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

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
 * TypeSafety then use DataSets
 * Fast performance then use DataFrames
 *
 * Subtle Note: DataFrame = Dataset[Row] i.e same thing but using the "Row" type
 */

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

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
   * TODO Exercises:
   * 1. Count how many cars we have
   * 2. Count how many powerful cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

}
