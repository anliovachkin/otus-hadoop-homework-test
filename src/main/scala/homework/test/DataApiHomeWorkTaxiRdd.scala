package homework.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import java.sql.Timestamp

object DataApiHomeWorkTaxiRdd extends App {

  implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  case class TaxiRide(tpep_pickup_datetime: Timestamp)

  import spark.implicits._

  implicit def ordered: Ordering[(Long, Timestamp)] =
    (x: (Long, Timestamp), y: (Long, Timestamp)) => {
      val result: Int = x._1 compareTo y._1
      if (result == 0) {
        x._2 compareTo y._2
      } else {
        result
      }
    }

  val rdd = rddFromParquet("src/main/resources/data/yellow_taxi_jan_25_2018").as[TaxiRide].rdd

  def rddFromParquet(path: String)(implicit spark: SparkSession) =
    spark.read.load(path)

  val resultRdd: RDD[(Int, Long)] = mostPopularPickupTime(rdd)
  val outputDF: Dataset[(Int, Long)] = resultRdd.toDF()
    .as[(Int, Long)]
  writeToTxt(outputDF)

  def mostPopularPickupTime(rdd: RDD[TaxiRide]) = {
    rdd
      .map(taxiRide => (taxiRide.tpep_pickup_datetime.toLocalDateTime.getHour, 1L))
      .reduceByKey((acc1: Long, acc2: Long) => acc1 + acc2)
      .sortBy(pair => (pair._2, pair._1), ascending = false)
  }

  private def writeToTxt(dataset: Dataset[(Int, Long)]): Unit = {
    dataset.show()
    dataset
      .map(pair => pair._1 + " " + pair._2)
      .write
      .option("sep", " ")
      .format("text")
      .save("outputRDD.txt")
  }
}