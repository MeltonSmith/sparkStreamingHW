package testUtils

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.joda.time.DateTime
import streaming.App._

import scala.collection.immutable

/**
 * Created by: Ian_Rakhmatullin
 * Date: 21.04.2021
 */
object TestUtils {
  def createDF(spark : SparkSession, seq: Seq[Row], schema: Seq[StructField]) = {
    val expediaInputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(seq),
      StructType(schema)
    )
    expediaInputDF
  }

  def createHotelDailyRow(hotel_id: Long, checkInDate: String, tmprC: Double) = {
    Row(hotel_id, checkInDate, tmprC, hotel_id + "/" + checkInDate)
  }

  def createHotelDailyRowAsInKafka(key: Long, value: String, timestamp: String) = {
    Row(key, value, timestamp)
  }

  def createExpediaRowFiltered(checkInDate: String, durationOfStay: Int, hotel_id: Long, childrenCount: Int) = {
    val checkOutTimeStr = DateTime.parse(checkInDate)
      .plusDays(durationOfStay)
      .toString("yyyy-MM-dd")

    Row(checkInDate, checkOutTimeStr, childrenCount, hotel_id, childrenCount > 0, durationOfStay, hotel_id + "/" + checkInDate)
  }

  def getTestExpediaFilteredSchema: immutable.Seq[StructField] = {
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

  def getTestHotelDailyInputSchema: immutable.Seq[StructField] = {
    List(
      StructField("id", LongType),
      StructField("wthr_date", StringType),
      StructField("avg_tmpr_c", DoubleType),
      StructField("key", StringType),
    )
  }

  def getTestStaticAggregationOutputSchema: immutable.Seq[StructField] = {
    List(
      StructField(hotel_id, LongType),
      StructField(withChildren, BooleanType),
      StructField(erroneous_data_cnt, IntegerType),
      StructField(short_stay_cnt, IntegerType),
      StructField(standard_stay_cnt, IntegerType),
      StructField(standard_extended_stay_cnt, IntegerType),
      StructField(long_stay_cnt, IntegerType),
      StructField(most_popular_stay_type, StringType),
    )
  }

  def getTestExpediaAggregationInputSchema: immutable.Seq[StructField] = {
    getTestExpediaFilteredSchema
  }
}
