package year2016Tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import model.VisitType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App
import testUtils.TestUtils
import testUtils.TestUtils.{getTestExpediaAggregationInputSchema, getTestExpediaOutputSchema, getTestHotelDailyInputSchema, getTestStaticAggregationOutputSchema}

/**
 * Created by: Ian_Rakhmatullin
 * Date: 21.04.2021
 */
class AggregationTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  it("should aggregate properly") {
    // srch_ci, srch_co, children_cnt, hotel_id, withChildren, durationOfStay, key
    val filteredExpedia = Seq(
      //for hotel 1
      Row("2016-08-04", "2016-08-05", 0, 111111111L, false, 1, "111111111/2016-08-04"),
      Row("2016-08-04", "2016-08-10", 0, 111111111L, false, 6,"111111111/2016-08-04"),

      //for hotel 2
      Row("2016-10-06", "2016-08-20", 1, 111111112L, true, -47, "111111112/2016-10-06"),
      Row("2016-10-06", "2016-10-21", 1, 111111112L, true, 15, "111111112/2016-10-06"),
      Row("2016-10-06", "2016-10-22", 1, 111111112L, true, 16, "111111112/2016-10-06"),
      Row("2016-10-06", "2016-10-23", 1, 111111112L, true, 17, "111111112/2016-10-06"),
      Row("2016-10-06", "2016-10-10", 1, 111111112L, true, 4, "111111112/2016-10-06")
    )


    // id(hotel_id), whr_date, avg_tmpr_c, key
    val hotelDailyDataForJoin = Seq(
      //hotelDaily for hotel 1
      Row(111111111L, "2016-08-05", 2.2, "111111111/2016-08-04"),
      Row(111111111L, "2016-08-10", 34.2, "111111111/2016-08-10"),

      //hotelDaily for hotel 2
      Row(111111112L, "2016-08-20", 1.02, "111111112/2016-10-06")
    )

    //TODO ignore batch_timestamp
//  hotel_id, with_children, erroneous_data_cnt, short_stay_cnt, standard_stay_cnt, standard_extended_stay_cnt, long_stay_cnt, most_popular_stay_type
    val aggregationResult = Seq(
      //aggregation result for hotel 1
      Row(111111111L, false, 0, 1, 1, 0, 0, VisitType.shortStayStr),

      //aggregation for hotel 2
      Row(111111112L, true, 1, 0, 1, 0, 3, VisitType.longStayStr)
    )


    //input
    val expediaRawDS = TestUtils.createDF(spark, filteredExpedia, getTestExpediaAggregationInputSchema)
    val hotelDailyDS = TestUtils.createDF(spark, hotelDailyDataForJoin, getTestHotelDailyInputSchema)

    val expectedAggregationResultDs = TestUtils.createDF(spark, aggregationResult, getTestStaticAggregationOutputSchema)

    //output
    val actualAggregationResult = App.getAggregatedResultFor2016(expediaRawDS, hotelDailyDS)(spark)

    assertSmallDatasetEquality(actualAggregationResult, expectedAggregationResultDs, ignoreNullable = true, orderedComparison = false)

    info("result DS for expedia 2016 contains all needed columns and omits the records for other years but 2016")
  }

}
