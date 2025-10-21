package part7_big_data_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Separate app for AWS EMR deployment
 */

object TaxiEconomicImpact {
  import spark.implicits._

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Taxi Big Data Application")
    .getOrCreate()

  // Suppress logs
  spark.sparkContext.setLogLevel("ERROR")

  val bigTaxiDF = spark.read.load("src/main/resources/data/bigdata_set/NYC_taxi_2009-2016.parquet")
  bigTaxiDF.printSchema()
  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  //  taxiZonesDF.printSchema()

  /**
   * - Main App
   */

  // Modeling
  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * bigTaxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
  val percentGroupable = 289623 * 1.0 / 331893 //taken from smaller dataset

  val groupAttemptsDF = bigTaxiDF
    .select(round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"), col("PULocationID"), col("total_amount"))
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg((count("*") * percentGroupable).as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .orderBy(col("total_trips").desc_nulls_last)
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")

  //  groupAttemptsDF.show()


  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - discount))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

  val totalEconomicImpactDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))


}
