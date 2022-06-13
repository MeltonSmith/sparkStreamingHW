package deduplication

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import hotelDailyData.RemoveDuplicatesAppNew
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField}
import org.scalatest.FunSpec
import sessionWrapper.SparkSessionTestWrapper
import testUtils.TestUtils
import testUtils.TestUtils.{createExpediaRowFiltered, createHotelDailyRowAsInKafka, getTestHotelDailyInputSchema}

import scala.collection.immutable

/**
 * Created by: mSmith
 * Date: 14.06.2022
 */
class DeduplicationTest extends FunSpec with SparkSessionTestWrapper with DatasetComparer{
  val firstHotelId = 111111111L
  val secondHotelId = 111111112L

  val date1 = "2016-08-04"
  val date2 = "2016-08-05"
  val date3 = "2016-08-10"
  val date4 = "2016-10-06"
  val date5 = "2016-10-07"
  val date6 = "2016-04-12"
  val date7 = "2016-04-13"

  it("should deduplicate") {
    val rawHotelDaily = Seq(
      //for hotel 1
      createHotelDailyRowAsInKafka(firstHotelId, "valueLast", date5),
      createHotelDailyRowAsInKafka(firstHotelId, "valueNotLast", date1),
      createHotelDailyRowAsInKafka(firstHotelId, "valueNotLast", date2),
      createHotelDailyRowAsInKafka(secondHotelId, "valueLastForSecondHotel", date5),
      createHotelDailyRowAsInKafka(secondHotelId, "valueNotLast", date1),
      createHotelDailyRowAsInKafka(secondHotelId, "valueNotLast", date7),
    )

    val hotelDailyRaw = TestUtils.createDF(spark, rawHotelDaily, getTestHotelDailyDedupSchema)


    RemoveDuplicatesAppNew.distinctByLastTimeStampForAGivenKey(hotelDailyRaw).show()
  }

  def getTestHotelDailyDedupSchema: immutable.Seq[StructField] = {
    List(
      StructField("key", LongType),
      StructField("value", StringType),
      StructField("timestamp", StringType)
    )
  }

}
