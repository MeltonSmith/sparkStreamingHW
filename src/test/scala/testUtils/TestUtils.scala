package testUtils

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

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

  def getTestExpediaOutputSchema: immutable.Seq[StructField] = {
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

  def getTestExpediaAggregationInputSchema: immutable.Seq[StructField] = {
    getTestExpediaOutputSchema
  }
}
