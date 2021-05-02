package homework.test

import homework.test.DataApiHomeWorkTaxiDataFrame.{calculateMostPopularBorough, loadFacts, loadZones}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class DataApiHomeWorkTaxiDataFrameTest extends SharedSparkSession {

  test("join - processTaxiData") {
    val taxiZonesDF2 = loadZones("src/main/resources/data/taxi_zones.csv")
    val taxiDF2 = loadFacts("src/main/resources/data/yellow_taxi_jan_25_2018")

    val actualDistribution = calculateMostPopularBorough(taxiZonesDF2, taxiDF2)

    checkAnswer(
      actualDistribution,
      Row("Manhattan", 296529) :: Row("Queens", 13822) :: Row("Brooklyn", 12673) ::
        Row("Unknown", 6714) :: Row("Bronx", 1590) :: Row("EWR", 508) :: Row("Staten Island", 67)
        :: Nil
    )
  }
}
