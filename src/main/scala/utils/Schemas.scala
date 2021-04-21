package utils

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField}
import streaming.App.hotel_id

/**
 * Created by: Ian_Rakhmatullin
 * Date: 21.04.2021
 */
object Schemas {
  def getExpediaInputSchema = {
    List(
      //      StructField("id", LongType),
      StructField("srch_ci", StringType),
      StructField("srch_co", StringType),
      StructField("srch_children_cnt", IntegerType),
      StructField(hotel_id, LongType),
    )
  }

  def getHotelDailyValueSchema = {
    List(
      StructField("Id", LongType, nullable = false),
      StructField("wthr_date", StringType, nullable = false),
      StructField("avg_tmpr_c", DoubleType, nullable = true),
      StructField("year", StringType, nullable = false),
      StructField("month", StringType, nullable = false),
      StructField("day", StringType, nullable = false),
    )
  }

}
