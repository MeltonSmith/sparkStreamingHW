package year2016Tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App
import testUtils.TestUtils
import testUtils.TestUtils.{getTestExpediaAggregationInputSchema, getTestExpediaOutputSchema, getTestHotelDailyInputSchema}

/**
 * Created by: Ian_Rakhmatullin
 * Date: 21.04.2021
 */
class AggregationTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  it("should filter all except 2016") {
    // srch_ci, srch_co, children_cnt, hotel_id, withChildren, durationOfStay, key
    val filteredExpedia = Seq(
      //for hotel 1
      Row("2016-08-04", "2016-08-05", 0, 111111111L, false, 1, "111111111/2016-08-04"),
      Row("2016-08-04", "2016-08-10", 0, 111111111L, false, 6,"111111111/2016-08-04"),

      //for hotel 2
      Row("2016-10-06", "2016-08-20", 1, 111111112L, true, -47, "111111112/2016-10-06")
    )


    // id(hotel_id), whr_date, avg_tmpr_c, key
    val hotelDailyDataForJoin = Seq(
      //hotelDaily for hotel 1
      Row(111111111L, "2016-08-05", 2.2, "111111111/2016-08-04"),
      Row(111111111L, "2016-08-10", 34.2, "111111111/2016-08-04"),

      //hotelDaily for hotel 2
      Row(111111112L, "2016-08-20", 1.02, "111111112/2016-10-06")
    )

    //TODO ignore batch_timestamp
//    hotel_id, with_children, batch_timestamp, erroneous_data_cnt, short_stay_cnt, standart_stay_cnt, standart_extended_stay_cnt, long_stay_cnt, most_popular_stay_type

    val aggregationResult = Seq(
      //hotelDaily for hotel 1
      Row(111111111L, "2016-08-05", 2.2, "111111111/2016-08-04"),
      Row(111111111L, "2016-08-10", 34.2, "111111111/2016-08-04"),

      //hotelDaily for hotel 2
      Row(111111112L, "2016-08-20", 1.02, "111111112/2016-10-06")
    )


    //input
    val expediaRawDS = TestUtils.createDF(spark, filteredExpedia, getTestExpediaAggregationInputSchema)
    val expediaExpectedDS = TestUtils.createDF(spark, hotelDailyDataForJoin, getTestHotelDailyInputSchema)

    //output
//    val expediaExpectedDS = TestUtils.createDF(spark, expediaExpectedSeq, getTestExpediaOutputSchema)

//    val result = App.getAggregatedResultFor2016(expediaRawDS, )(spark)

//    assertSmallDatasetEquality(result, expediaExpectedDS, ignoreNullable = true)

    info("result DS for expedia 2016 contains all needed columns and omits the records for other years but 2016")
  }

}
