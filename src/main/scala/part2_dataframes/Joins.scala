package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

/**
 * Joins
 * - Combine data from multiple DataFrames
 * - one (or more) columns from table 1 (left) is compared with one (or more) columns from table 2 (right)
 * ---> If the condition passes, rows are combined
 * ---> Non matching rows are discarded
 * Important: These are wide transformations. i.e very expensive.
 */

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  //--------------------------------------------------------------------

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id") // For code reuse
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  guitaristsDF.show

  // outer joins
  // left outer = everything in the inner join and all the rows in the left table, with nulls where the data is missing.
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in the inner join and all the rows in the right table, with nulls where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // (full) outer join = everything in the inner join and all the rows in both tables, with nulls where the data is missing
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DataFrame for which there is a row in the right DataFrame satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-joins = everything in the left DataFrame for which there is no row in the right DataFrame satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // Something to note
  // guitaristsBandsDF.select("id", "band").show // this crashes because Spark does not know which "id" is being referred to

  //How to fix this
  // Option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // Option 2 - drop the duplicate column --- need to specify which table is being referred to
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // Option 3 - rename one of the columns and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types like arrays
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /**
   * TODO: Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */



}
