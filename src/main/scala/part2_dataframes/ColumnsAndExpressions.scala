package part2_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App{

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting) - taking a dataframe projecting it into a new dataframe which containes less data. (See PCA)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods for selecting parts of a data frame
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // expression
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  /**
   * Narrow transformation - After select we get a new DataFrame with less columns. Every input data frame has exactly one output partition in the dataframe. This is reflected across all clusters.
   */

  // Expressions: will allow you to process data frames in any way you like
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // Processing data frames
  // Adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // Renaming an existing column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // Caution with column names -- backs ticks required if there are spaces in the column name
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // Remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")
  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150) //Triple equals for Spark
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)  // Chain filters. Infix for readability if required.
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150") // Pass filter in the same string

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()

}
