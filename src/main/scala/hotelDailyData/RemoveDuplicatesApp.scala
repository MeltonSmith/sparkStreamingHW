package hotelDailyData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number}
import streaming.App.hotelsWeatherTopic

/**
 * Removes duplicates from hotelDailyData, taking only the last values by the timestamp
 *
 * Created by: Ian_Rakhmatullin
 * Date: 13.04.2021
 */
object RemoveDuplicatesApp {
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
                              .select("key", "value", "timestamp")
                              .withColumn("row_number", row_number().over(Window.partitionBy("key")
                                                                                          .orderBy(desc("timestamp"))))
                              .filter(col("row_number").equalTo(1))

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


