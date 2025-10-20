package part4_spark_sql

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}
import part3_types_and_datasets.CommonTypes.spark

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse") //URI to "Data Warehouse"
    .getOrCreate()

  // Suppress logs
  spark.sparkContext.setLogLevel("ERROR")

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL API
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
    """.stripMargin)

  spark.sql("DROP DATABASE IF EXISTS rtjvm CASCADE")
  // we can run any SQL statement
  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")
  val databasesDF = spark.sql("show databases")

  /** transfer tables from a DB to Spark tables */
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

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
    , shouldWriteToWarehouse = false // saves to parquet file
  )

  // read DataFrame from loaded Spark tables / data warehouse
  val employeesDF2 = spark.read.table("employees")
  employeesDF2.show()

  /**
   * Exercises
   *
   * 1) - Read the movies DF and store it as a Spark table in the rtjvm database
   * 2) - Count how many employees were hired in between Jan 1 1999 and Jan 1 2000
   * 3) - Show the average salaries for the employees hired in between those dates, grouped by department
   * 4) - Show the name of the best-paying department for employees hired in between those dates
   *
   */

  // 1 - Reading a DF and store it as a spark table
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  // 2 - Count query
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
    """.stripMargin
  ).show()

  // 3 - Group by query
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
    """.stripMargin
  ).show()

  // 4
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
    """.stripMargin
  ).show()

}

