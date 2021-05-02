package homework.test

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object DataApiHomeWorkTaxiDataSet extends App {

  implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/otus"
  val user = "docker"
  val password = "docker"
  val table = "public.taxi_trip_agg"

  import spark.implicits._

  case class TaxiRide(trip_distance: Double)

  val taxiFacts = loadFactsFromParquet.as[TaxiRide]
  writeToPostgres(aggregateByDistance(taxiFacts))

  def loadFactsFromParquet(implicit spark: SparkSession) = {
    spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  }

  def aggregateByDistance(dataset: Dataset[TaxiRide]) = {
    dataset.agg(max(col("trip_distance")), min(col("trip_distance")),
      stddev(col("trip_distance")), count(col("trip_distance")),
      avg(col("trip_distance")))
      .withColumnRenamed("max(trip_distance)", "max_distance")
      .withColumnRenamed("min(trip_distance)", "min_distance")
      .withColumnRenamed("stddev_samp(trip_distance)", "stddev_distance")
      .withColumnRenamed("count(trip_distance)", "total_trips")
      .withColumnRenamed("avg(trip_distance)", "avg_distance")
  }

  private def writeToPostgres(dataFrame: DataFrame): Unit = {
    dataFrame.show()
    dataFrame.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", user)
      .option("password", password)
      .mode(SaveMode.Append)
      .save()
  }
}