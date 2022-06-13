package hotelDailyData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, max, row_number}
import streaming.App.hotelsWeatherTopic

/**
 * Created by: mSmith
 * Date: 14.06.2022
 */
object RemovingDuplicatesAppNew {

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
      .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "CAST(timestamp AS Timestamp) as timestamp")
      .groupBy("key")
      .agg(max("timestamp"))


    hotelDailyKafka
      .select("key", "value")
      .write
      .option("kafka.bootstrap.servers", "localhost:9094")
      .format("kafka")
      .option("topic", hotelDailyDataUnique)
      .save()

    spark.close()
  }

}
