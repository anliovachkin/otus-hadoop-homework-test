package homework.test

import homework.test.DataApiHomeWorkTaxiDataSet.{TaxiRide, aggregateByDistance, loadFactsFromParquet}
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.SharedSparkSession

class DataApiHomeWorkTaxiDataSetTest extends SharedSparkSession {
  import testImplicits._

  test("aggregate taxi data by distance") {
    val taxiFacts = loadFactsFromParquet.as[TaxiRide]

    val actualDistribution = aggregateByDistance(taxiFacts)

    checkAnswer(
      actualDistribution,
        Row(66.0, 0.0, 3.485152224885052, 331893, 2.717989442380494) :: Nil
    )
  }
}
