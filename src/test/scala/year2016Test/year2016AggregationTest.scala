package year2016Test

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App
import streaming.App.getExpediaInputSchema

/**
 * Created by: Ian_Rakhmatullin
 * Date: 20.04.2021
 */
class year2016AggregationTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{

  it("should filter") {
    // srch_ci, srch_co, children_cnt, hotel_id
    val expediaRaw = Seq(
      //hotel 1
      Row("2020-08-04", "2020-08-05", 0, 111111111L),
      Row("2016-08-04", "2016-08-05", 0, 111111111L),
      Row("2016-08-04", "2016-08-10", 0, 111111111L),

      //hotel 2
      Row("2016-10-06", "2016-08-20", 1, 111111112L),
      Row("2017-11-12", "2016-08-20", 1, 111111112L)
    )

    val expediaExpectedSeq = Seq(
      //hotel 1
      Row("2016-08-04", "2016-08-05", 0, 111111111L, false, 1, "111111111/2016-08-04"),
      Row("2016-08-04", "2016-08-10", 0, 111111111L, false, 6,"111111111/2016-08-04"),

      //hotel 2
      Row("2016-10-06", "2016-08-20", 1, 111111112L, true, -47, "111111112/2016-10-06")
    )


    val expediaRawDS = createDF(expediaRaw, getExpediaInputSchema)
    val expediaExpectedDS = createDF(expediaExpectedSeq, getExpediaOutputSchema)


    val result = App.filterStaticExpedia(expediaRawDS)

    assertSmallDatasetEquality(result, expediaExpectedDS, ignoreNullable = true)

    info("should contain all the records test is passed")
  }

  private def createDF(seq: Seq[Row], schema: Seq[StructField]) = {
    val expediaInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(seq),
      StructType(schema)
    )
    expediaInputDF
  }

  private def getExpediaOutputSchema = {
    List(
      StructField("srch_ci", StringType),
      StructField("srch_co", StringType),
      StructField("srch_children_cnt", IntegerType),
      StructField("hotel_id", LongType),
      StructField("withChildren", BooleanType),
      StructField("durationOfStay", IntegerType),
      StructField("key", StringType),
    )
  }
}
