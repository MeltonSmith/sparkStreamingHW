package year2016Tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App
import testUtils.TestUtils
import testUtils.TestUtils.{createExpediaRowFiltered, getTestExpediaFilteredSchema}
import utils.Schemas

/**
 * Created by: Ian_Rakhmatullin
 * Date: 20.04.2021
 *
 * All records for years expect 2016 should be filtered out
 *
 */
class ExpediaFilteringTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  val date1 = "2020-08-04"
  val date2 = "2016-08-04" //ok
  val date3 = "2016-10-06" //ok
  val date4 = "2017-11-12"

  val hotelId1 = 111111111L
  val hotelId2 = 111111112L

  it("should filter all except 2016 and create needed columns") {


    // srch_ci, srch_co, children_cnt, hotel_id
    val expediaRaw = Seq(
      //for hotel 1
      Row(date1, "2020-08-05", 0, hotelId1),
      Row(date2, "2016-08-05", 0, hotelId1),
      Row(date2, "2016-08-10", 0, hotelId1),

      //for hotel 2
      Row(date3, "2016-08-20", 1, hotelId2),
      Row(date4, "2016-08-20", 1, hotelId2)
    )

    // srch_ci, srch_co, children_cnt, hotel_id, withChildren, durationOfStay, key
    val expediaExpectedSeq = Seq(
      //for hotel 1
      createExpediaRowFiltered(date2, 1, hotelId1, 0),
      createExpediaRowFiltered(date2, 6, hotelId1, 0),

      //for hotel 2
      createExpediaRowFiltered(date3, -47, hotelId2, 1)
    )

    val expediaRawDS = TestUtils.createDF(spark, expediaRaw, Schemas.getExpediaInputSchema)
    val expediaExpectedDS = TestUtils.createDF(spark, expediaExpectedSeq, getTestExpediaFilteredSchema)

    val result = App.filterStaticExpedia(expediaRawDS)

    assertSmallDatasetEquality(result, expediaExpectedDS, ignoreNullable = true)

    info("result DS for expedia 2016 contains all needed columns and omits the records for other years but 2016")
  }




}
