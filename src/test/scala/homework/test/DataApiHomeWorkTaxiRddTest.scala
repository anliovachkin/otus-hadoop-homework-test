package homework.test

import homework.test.DataApiHomeWorkTaxiRdd.{TaxiRide, mostPopularPickupTime, rddFromParquet}
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class DataApiHomeWorkTaxiRddTest extends AnyFlatSpec {

  implicit val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Test №1 for Big Data Application")
    .getOrCreate()
  import spark.implicits._

  it should "upload and process data" in {

    val expectedResult = Array((19, 22121), (20, 21598), (22, 20884), (21, 20318), (23, 19528),
      (9, 18867), (18, 18664), (16, 17843), (15, 17483), (10, 16840), (17, 16160), (14, 16082),
      (13, 16001), (12, 15564), (8, 15445), (11, 15348), (0, 14652), (7, 8600), (1, 7050),
      (2, 3978), (6, 3133), (3, 2538), (4, 1610), (5, 1586))

    val taxiRdd = rddFromParquet("src/main/resources/data/yellow_taxi_jan_25_2018")
      .as[TaxiRide]
      .rdd
    val actualDistribution = mostPopularPickupTime(taxiRdd).collect()

    actualDistribution should contain theSameElementsInOrderAs expectedResult
  }

}
