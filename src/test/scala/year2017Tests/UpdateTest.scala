package year2017Tests

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import model.VisitType
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{Encoders, Row, SQLContext}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import streaming.App.getFinalResultDS
import testUtils.TestUtils
import testUtils.TestUtils.{createExpediaRowForJoin, createHotelDailyRow, getTestExpediaAggregationInputSchema, getTestExpediaOutputSchema, getTestHotelDailyInputSchema, getTestStaticAggregationOutputSchema}

/**
 * Created by: Ian_Rakhmatullin
 * Date: 21.04.2021
 */
class UpdateTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{
  implicit val sqlCtx: SQLContext = spark.sqlContext




  val firstHotelId = 111111111L
  val secondHotelId = 111111112L

  val date1 = "2017-09-12"
  val date2 = "2017-09-15"
  val date3 = "2017-10-03"
  val date4 = "2017-01-06"
  val date5 = "2017-12-07"
  val date6 = "2017-07-12"
  val date7 = "2017-11-16"




  it("should join and aggregate properly") {

    implicit val value: ExpressionEncoder[Row] = RowEncoder(StructType(getTestExpediaOutputSchema))
    val events = MemoryStream[Row]
    val sessions = events.toDS

    val filteredExpediaFor2017 = Seq(
      //for hotel 1
//      createExpediaRowForJoin(date1, 1, firstHotelId, 0),
//      createExpediaRowForJoin(date1, 6, firstHotelId, 0),
      createExpediaRowForJoin(date2, 10, firstHotelId, 0),
      createExpediaRowForJoin(date3, 11, firstHotelId, 0),

      //for hotel 2 with children
//      createExpediaRowForJoin(date4, -47, secondHotelId, 1),
//      createExpediaRowForJoin(date4, 15, secondHotelId, 20),
      createExpediaRowForJoin(date4, 10, secondHotelId, 42),
      createExpediaRowForJoin(date4, 8, secondHotelId, 13),
//      createExpediaRowForJoin(date4, 4, secondHotelId, 1),
      //without children for hotel 2, but can be joint
      createExpediaRowForJoin(date4, 1, secondHotelId, 0),
//      createExpediaRowForJoin(date5, 10, secondHotelId, 0),

      //some non joint data for hotel2 (weather data was filtered somehow)
      createExpediaRowForJoin(date6, 1, secondHotelId, 0),
      createExpediaRowForJoin(date7, 2, secondHotelId, 0),
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

    val updateResults = Seq(
      //aggregation results for hotel 1
      Row(firstHotelId, false, 2, 3, 13, 14, 4, VisitType.standardExStr), //type should be changed

      //aggregation for hotel 2
      Row(secondHotelId, true, 2, 3, 4, 17, 16, VisitType.standardExStr), //type should be changed
      Row(secondHotelId, false, 0, 201, 199, 23, 54, VisitType.shortStayStr) //type stays as is

    )

    val queryName = "2017data"
    events.addData(filteredExpediaFor2017)

    val streamingQuery = sessions
                          .writeStream
                          .format("memory")
                          .queryName(queryName)
                          .outputMode("complete")
                          .start

    streamingQuery.processAllAvailable()
    //input
    val expediaFor2017Streamed = spark.sql(s"select * from $queryName")
    val hotelDailyDS = TestUtils.createDF(spark, hotelDailyDataForJoin2017, getTestHotelDailyInputSchema)
    val aggregationResultsFor2016DS = TestUtils.createDF(spark, aggregationResultFor2016, getTestStaticAggregationOutputSchema)

    //output
    //TODO make expected generified with hotelState
    val updateExpectedResultsDS = TestUtils.createDF(spark, updateResults, getTestStaticAggregationOutputSchema)
    val actualResultOfUpdate = getFinalResultDS(expediaFor2017Streamed, hotelDailyDS, aggregationResultsFor2016DS)(spark)

    assertSmallDatasetEquality(actualResultOfUpdate, updateExpectedResultsDS, ignoreNullable = true, orderedComparison = false)



//    val rsvpEventName = spark.sql("select hotel_id from 2017data")
//      .collect()
//      .map(_.getAs[Long](0))
//      .foreach(a => println(a))
//    val updateExpectedResults = TestUtils.createDF(spark, aggregationResultFor2016, getTestStaticAggregationOutputSchema)



  }


}
