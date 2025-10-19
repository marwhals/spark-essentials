package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, max}

/**
 * Joins
 * - Combine data from multiple DataFrames
 * - one (or more) columns from table 1 (left) is compared with one (or more) columns from table 2 (right)
 * ---> If the condition passes, rows are combined
 * ---> Non-matching rows are discarded
 * Important: These are wide transformations. i.e. very expensive.
 */

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  // Suppress logs
  spark.sparkContext.setLogLevel("ERROR")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars_data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars_data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars_data/bands.json")

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
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  employeesSalariesDF.show()

  //2
  val empNeverManagersDF = employeesDF.join(
    deptManagersDF,
    employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
    "left_anti"
  )
  empNeverManagersDF.show(Int.MaxValue, truncate = false)
  //  val df_pandas = empNeverManagersDF.toPandas // requires spark update

  // 3
  val mostRecentJobTitlesDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF, "emp_no")
  bestPaidJobsDF.show()

}
