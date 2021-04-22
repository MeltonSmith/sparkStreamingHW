package year2016Tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import model.VisitType
import org.apache.spark.sql.Row
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App
import testUtils.TestUtils
import testUtils.TestUtils._

/**
 * Created by: Ian_Rakhmatullin
 * Date: 21.04.2021
 *
 * Test for aggregation which is done by getAggregatedResultForASingleYear.
 *
 */
class AggregationTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{
  val firstHotelId = 111111111L
  val secondHotelId = 111111112L

  val date1 = "2016-08-04"
  val date2 = "2016-08-05"
  val date3 = "2016-08-10"
  val date4 = "2016-10-06"
  val date5 = "2016-10-07"
  val date6 = "2016-04-12"
  val date7 = "2016-04-13"

  it("should join and aggregate properly") {

    // srch_ci, srch_co, children_cnt, hotel_id, withChildren, durationOfStay, key
    val filteredExpedia = Seq(
      //for hotel 1
      createExpediaRowFiltered(date1, 1, firstHotelId, 0),
      createExpediaRowFiltered(date1, 6, firstHotelId, 0),
      createExpediaRowFiltered(date2, 10, firstHotelId, 0),
      createExpediaRowFiltered(date3, 11, firstHotelId, 0),

      //for hotel 2 with children
      createExpediaRowFiltered(date4, -47, secondHotelId, 1),
      createExpediaRowFiltered(date4, 15, secondHotelId, 20),
      createExpediaRowFiltered(date4, 16, secondHotelId, 42),
      createExpediaRowFiltered(date4, 17, secondHotelId, 13),
      createExpediaRowFiltered(date4, 4, secondHotelId, 1),
        //without for hotel 2, but can be joint
      createExpediaRowFiltered(date4, 1, secondHotelId, 0),
      createExpediaRowFiltered(date5, 10, secondHotelId, 0),

        //some non joint data for hotel2 (weather data was filtered somehow)
      createExpediaRowFiltered(date6, 1, secondHotelId, 0),
      createExpediaRowFiltered(date7, 2, secondHotelId, 0),
    )

    // id(hotel_id), whr_date, avg_tmpr_c, key
    val hotelDailyDataForJoin = Seq(
      //hotelDaily for hotel 1
      createHotelDailyRow(firstHotelId, date1, 2.2),
      createHotelDailyRow(firstHotelId, date2, 10.1),
      createHotelDailyRow(firstHotelId, date3, 12.3),

      //hotelDaily for hotel 2
      createHotelDailyRow(secondHotelId, date4, 1.02),
      createHotelDailyRow(secondHotelId, date5, 0.5)
    )

//  hotel_id, with_children, erroneous_data_cnt, short_stay_cnt, standard_stay_cnt, standard_extended_stay_cnt, long_stay_cnt, most_popular_stay_type
    val aggregationResult = Seq(
      //aggregation result for hotel 1
      Row(firstHotelId, false, 0, 1, 1, 2, 0, VisitType.standardExStr),

      //aggregation for hotel 2
      Row(secondHotelId, true, 1, 0, 1, 0, 3, VisitType.longStayStr),
      Row(secondHotelId, false, 0, 1, 0, 1, 0, VisitType.shortStayStr)
    )


    //input
    val expediaRawDS = TestUtils.createDF(spark, filteredExpedia, getTestExpediaAggregationInputSchema)
    val hotelDailyDS = TestUtils.createDF(spark, hotelDailyDataForJoin, getTestHotelDailyInputSchema)

    val expectedAggregationResultDs = TestUtils.createDF(spark, aggregationResult, getTestStaticAggregationOutputSchema)

    //output
    val actualAggregationResult = App.getAggregatedResultForASingleYear(expediaRawDS, hotelDailyDS)(spark)

    assertSmallDatasetEquality(actualAggregationResult, expectedAggregationResultDs, ignoreNullable = true, orderedComparison = false)

    info("result DS for aggregation of a single year is correct")
  }


}
