package homework.test

import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object DataApiHomeWorkTaxiDataFrame extends App {

 implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"

  writeToParquet(calculateMostPopularBorough(
    loadFacts("src/main/resources/data/yellow_taxi_jan_25_2018"),
    loadZones("src/main/resources/data/taxi_zones.csv")))

  def loadFacts(path: String)(implicit spark: SparkSession) = {
    spark.read.load(path)
  }

  def loadZones(path: String)(implicit spark: SparkSession) = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }

  def calculateMostPopularBorough(facts: DataFrame, zones: DataFrame) = {
    facts.join(broadcast(zones), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .count()
      .orderBy(col("count").desc)
  }

  private def writeToParquet(dataFrame: DataFrame): Unit = {
    dataFrame.show()
    dataFrame.write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save("popularBorough")
  }
}