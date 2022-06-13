package hotelDailyData

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, expr, max, rank, row_number}
import streaming.App.hotelsWeatherTopic

/**
 * Removes duplicates from hotelDailyData, taking only the last values by the timestamp
 *
 * Created by: Ian_Rakhmatullin
 * Date: 14.06.2022
 */
object RemoveDuplicatesAppNew {
  val hotelDailyDataUnique = "hotelDailyDataUnique"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("sparkHotelDailyData")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val hotelDailyKafka = spark.read
                              .format("kafka")
                              .option("kafka.bootstrap.servers", "localhost:9094")
                              .option("startingOffsets", "earliest")
                              .option("subscribe", hotelsWeatherTopic)
                              .load()


    val distinctHotelDaily = distinctByLastTimeStampForAGivenKey(hotelDailyKafka)

    distinctHotelDaily
        .select("key", "value")
        .write
        .option("kafka.bootstrap.servers", "localhost:9094")
        .format("kafka")
        .option("topic", hotelDailyDataUnique)
        .save()

    spark.close()
  }

  def distinctByLastTimeStampForAGivenKey(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "CAST(timestamp AS Timestamp) as timestamp")
      .select("key", "value", "timestamp")
      .withColumn("row_number", row_number().over(Window.partitionBy("key").orderBy(desc("timestamp"))))
      .filter(expr("row_number == 1"))
      .drop("row_number")
  }

}


