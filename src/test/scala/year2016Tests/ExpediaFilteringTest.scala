package year2016Tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App
import testUtils.TestUtils
import testUtils.TestUtils.getTestExpediaOutputSchema
import utils.Schemas

/**
 * Created by: Ian_Rakhmatullin
 * Date: 20.04.2021
 */
class ExpediaFilteringTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  it("should filter all except 2016 and create needed columns") {
    // srch_ci, srch_co, children_cnt, hotel_id
    val expediaRaw = Seq(
      //for hotel 1
      Row("2020-08-04", "2020-08-05", 0, 111111111L),
      Row("2016-08-04", "2016-08-05", 0, 111111111L),
      Row("2016-08-04", "2016-08-10", 0, 111111111L),

      //for hotel 2
      Row("2016-10-06", "2016-08-20", 1, 111111112L),
      Row("2017-11-12", "2016-08-20", 1, 111111112L)
    )

    // srch_ci, srch_co, children_cnt, hotel_id, withChildren, durationOfStay, key
    val expediaExpectedSeq = Seq(
      //for hotel 1
      Row("2016-08-04", "2016-08-05", 0, 111111111L, false, 1, "111111111/2016-08-04"),
      Row("2016-08-04", "2016-08-10", 0, 111111111L, false, 6,"111111111/2016-08-04"),

      //for hotel 2
      Row("2016-10-06", "2016-08-20", 1, 111111112L, true, -47, "111111112/2016-10-06")
    )

    val expediaRawDS = TestUtils.createDF(spark, expediaRaw, Schemas.getExpediaInputSchema)
    val expediaExpectedDS = TestUtils.createDF(spark, expediaExpectedSeq, getTestExpediaOutputSchema)

    val result = App.filterStaticExpedia(expediaRawDS)

    assertSmallDatasetEquality(result, expediaExpectedDS, ignoreNullable = true)

    info("result DS for expedia 2016 contains all needed columns and omits the records for other years but 2016")
  }




}
