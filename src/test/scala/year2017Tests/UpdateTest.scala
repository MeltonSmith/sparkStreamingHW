package year2017Tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import model.{HotelState, VisitType}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}
import org.joda.time.DateTime
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App.getFinalResultDS
import testUtils.TestUtils
import testUtils.TestUtils._

import java.sql.Timestamp

/**
 * Created by: Ian_Rakhmatullin
 * Date: 21.04.2021
 *
 * Test for updating old data (previous year) with new data.
 *
 */
class UpdateTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{
  implicit val sqlCtx: SQLContext = spark.sqlContext
  import spark.implicits._

  val firstHotelId = 111111111L
  val secondHotelId = 111111112L

  val date1 = "2017-09-12"
  val date2 = "2017-09-15"
  val date3 = "2017-10-03"
  val date4 = "2017-01-06"
  val date5 = "2017-12-07"
  val date6 = "2017-07-12"
  val date7 = "2017-11-16"

  it("should update 2016 with 2017 data properly") {

    implicit val value: ExpressionEncoder[Row] = RowEncoder(StructType(getTestExpediaFilteredSchema))
    val events = MemoryStream[Row]
    val sessions = events.toDS

    val filteredExpediaFor2017 = Seq(
      //for hotel 1 adding 2 standard extended
      createExpediaRowFiltered(date2, 10, firstHotelId, 0),
      createExpediaRowFiltered(date3, 11, firstHotelId, 0),

      //for hotel 2 with children adding 2 standard extended
      createExpediaRowFiltered(date4, 10, secondHotelId, 42),
      createExpediaRowFiltered(date4, 8, secondHotelId, 13),
      //without children for hotel 2, but can be joint. Adding 1 short stay
      createExpediaRowFiltered(date4, 1, secondHotelId, 0),

      //some non joint data for hotel2 (weather data was filtered somehow)
      createExpediaRowFiltered(date6, 1, secondHotelId, 0),
      createExpediaRowFiltered(date7, 2, secondHotelId, 0),
    )

    val hotelDailyDataForJoin2017 = Seq(
      //hotelDaily for hotel 1
      createHotelDailyRow(firstHotelId, date1, 13.2),
      createHotelDailyRow(firstHotelId, date2, 11.1),
      createHotelDailyRow(firstHotelId, date3, 5.3),

      //hotelDaily for hotel 2
      createHotelDailyRow(secondHotelId, date4, 6.02),
      createHotelDailyRow(secondHotelId, date5, 12.5)
    )

    val aggregationResultFor2016 = Seq(
      //aggregation results for hotel 1
      Row(firstHotelId, false, 2, 3, 13, 12, 4, VisitType.standardExStr),

      //aggregation for hotel 2
      Row(secondHotelId, true, 2, 3, 4, 15, 16, VisitType.longStayStr),
      Row(secondHotelId, false, 0, 200, 199, 23, 54, VisitType.shortStayStr)

    )

    val dummyTimestamp = new Timestamp(new DateTime().getMillis)

    val updateResults2 = Seq(
      //aggregation results for hotel 1
      HotelState(firstHotelId, withChildren = false, dummyTimestamp, 2, 3, 13, 14, 4, VisitType.standard_extended_stay), //type should be changed

      //aggregation for hotel 2
      HotelState(secondHotelId, withChildren = true, dummyTimestamp, 2, 3, 4, 17, 16, VisitType.standard_extended_stay), //type should be changed
      HotelState(secondHotelId, withChildren = false, dummyTimestamp, 0, 201, 199, 23, 54, VisitType.short_stay) //type stays as is

    )

    val queryName = "2017data"
    events.addData(filteredExpediaFor2017)

    val streamingQuery = sessions
                          .writeStream
                          .format("memory")
                          .queryName(queryName)
                          .outputMode("update")
                          .start

    streamingQuery.processAllAvailable()
    //input
    val expediaFor2017Streamed = spark.sql(s"select * from $queryName")
    val hotelDailyDS = TestUtils.createDF(spark, hotelDailyDataForJoin2017, getTestHotelDailyInputSchema)
    val aggregationResultsFor2016DS = TestUtils.createDF(spark, aggregationResultFor2016, getTestStaticAggregationOutputSchema)

    //output
    val updateExpectedResultsDS = spark.createDataset(updateResults2)
    val actualResultOfUpdate = getFinalResultDS(expediaFor2017Streamed, hotelDailyDS, aggregationResultsFor2016DS)(spark)


    //ignoring timestamps
    assertSmallDatasetEquality(actualResultOfUpdate.drop("batch_timestamp"),
                              updateExpectedResultsDS.drop("batch_timestamp"),
                              ignoreNullable = true,
                              orderedComparison = false)

    info("result DS for update with 2017 year seems to be correct")

  }


}
