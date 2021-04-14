package streaming

import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions.{col, concat, count, current_timestamp, expr, from_json, greatest, lit, schema_of_json, sum, when, window}
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import scala.concurrent.duration.DurationInt

/**
 *
 * Perform Spark stateful streaming application. Use Spark Structured Streaming OR DStreams approach.

Note: In case of Spark Structure Streaming you can broadcast initial state map due to relatively small data size.

    Install and start Spark in WSL2. Use "Spark WSL2 Setup" guide for this. Or reuse Spark deployment from previous task.
    Read Expedia data for 2016 year from HDFS on WSL2 and enrich it with weather: add average temperature at checkin (join with hotels+weaher data from Kafka topic).
    Filter incoming data by having average temperature more than 0 Celsius degrees.
    Calculate customer's duration of stay as days between requested check-in and check-out date.
    Create customer preferences of stay time based on next logic.
        Map each hotel with multi-dimensional state consisting of record counts for each type of stay:
            "Erroneous data": null, more than month(30 days), less than or equal to 0
            "Short stay": 1 day stay
            "Standart stay": 2-7 days
            "Standart extended stay": 1-2 weeks
            "Long stay": 2-4 weeks (less than month)
        Add most_popular_stay_type for a hotel (with max count)
    Store it as initial state (For examples: hotel, batch_timestamp, erroneous_data_cnt, short_stay_cnt, standart_stay_cnt, standart_extended_stay_cnt, long_stay_cnt, most_popular_stay_type).
    In streaming way read Expedia data for 2017 year from HDFS on WSL2. Read initial state, send it via broadcast into streaming. Repeat previous logic on the stream.
    Apply additional variance with filter on children presence in booking (with_children: false - children <= 0; true - children > 0).
    Store final data in HDFS. (Result will look like: hotel, with_children, batch_timestamp, erroneous_data_cnt, short_stay_cnt, standart_stay_cnt, standart_extended_stay_cnt, long_stay_cnt, most_popular_stay_type

 *
 * Created by: Ian_Rakhmatullin
 * Date: 11.04.2021
 */
object App {

  val hotelsWeatherTopic = "hotelDailyDataUnique"
  val yearForExpedia = "2016"
  val durationOfStay = "durationOfStay"

  def main(args : Array[String]) {
    val spark = SparkSession
                    .builder
                    .appName("sparkStreamingHW2")
                    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    import spark.implicits._

    //TODO works
    val expedia = spark.readStream
                      .format("avro")
                      .schema(StructType(getExpediaInputSchema))
                      .load("/201 HW Dataset/expedia")


    //TODO persist?


    val expedia2016 = expedia
                        .where("year(CAST(srch_ci AS DATE)) == " + yearForExpedia)
                        .withColumn(durationOfStay, expr("(day(CAST(srch_co AS DATE))) - (day(CAST(srch_ci AS DATE)))"))
                        .withColumn("key", concat(col("hotel_id"),
                                                                        lit("/"),
                                                                        col("srch_ci")))

    val hotelDailyKafka = spark.readStream
                          .format("kafka")
                          .option("kafka.bootstrap.servers", "localhost:9094")
                          .option("startingOffsets", "earliest")
                          .option("subscribe", hotelsWeatherTopic)
                          .load()
                          .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "CAST(timestamp AS Timestamp) as timestamp")
                          .withColumn("jsonData", from_json(col("value"), StructType(getHotelDailyValueSchema))).as("data")
                          .select("key", "jsonData.*", "timestamp")
                          .where(col("avg_tmpr_c").isNotNull
                                .and
                                (col("avg_tmpr_c").gt(0))
                          )

    //enriching with weather
    val joinResult = expedia2016.as("exp")
            .join(hotelDailyKafka.as("hotelDaily"),
              $"hotelDaily.key" === $"exp.key"
            )
//      .withWatermark("timestamp", "2 seconds")
      .select("exp.*", "hotelDaily.avg_tmpr_c")
//      .groupBy("hotel_id", "timestamp").count()
//      .groupBy( window($"timestamp", "2 seconds")).count()
//      .writeStream
//                      .outputMode("append")
//                      .format("console")
//                      .start()


    val erroneousCondition = col(durationOfStay).isNull
                      .or(col(durationOfStay).gt(30))
                      .or(col(durationOfStay).leq(0))

    val shortStayCond = col(durationOfStay).equalTo(1)
    val standardStayCond = col(durationOfStay).between(2, 7)
    val standardExtendedStayCond = col(durationOfStay).between(8, 14)
    val longStayCond = col(durationOfStay).between(15, 29)


    val greatestAmongTheCounts = greatest("erroneous_data_cnt", "short_stay_cnt", "standard_stay_cnt", "standard_extended_stay_cnt", "long_stay_cnt")
    //TODO get rid of the hardcode below
    val query = joinResult
                  .withColumn("batch_timestamp", current_timestamp())
                  .withWatermark("batch_timestamp", "0 seconds")
                  .groupBy("hotel_id", "batch_timestamp")
                  .agg(
                    count(when(erroneousCondition, true)).as("erroneous_data_cnt"),
                    count(when(shortStayCond, true)).as("short_stay_cnt"),
                    count(when(standardStayCond, true)).as("standard_stay_cnt"),
                    count(when(standardExtendedStayCond, true)).as("standard_extended_stay_cnt"),
                    count(when(longStayCond, true)).as("long_stay_cnt")
                  )
                  .withColumn("most_popular_stay_type",
                      when(greatestAmongTheCounts === $"short_stay_cnt", "Short Stay")
                        .when(greatestAmongTheCounts === $"standard_stay_cnt", "Standard Stay")
                        .when(greatestAmongTheCounts === $"standard_extended_stay_cnt", "Standard Extended Stay")
                        .when(greatestAmongTheCounts === $"long_stay_cnt", "Long Stay")
                        .otherwise("Erroneous")
                    )
                  .writeStream
                  .trigger(Trigger.ProcessingTime("10 seconds"))
                  .outputMode("append")
                  .format("console")
                  .start()


//    val query = hotelDailyKakfa
//                            .writeStream
//                            .outputMode("append")
//                            .format("console")
//                            .option("truncate", value = false)
//                            .start()

//    val query = joinResult
//              .writeStream
//              .outputMode("append")
//              .format("console")
//              .start()

    query.awaitTermination()






//    spark.readStream
//                      .json(hotelDailyKakfa.selectExpr("CAST(value as STRING) as value")
//                                      .map(row => row.toString()))
//                                      .as

  }

  private def getExpediaInputSchema = {
    List(
      StructField("id", LongType),
      StructField("srch_ci", StringType),
      StructField("srch_co", StringType),
      StructField("hotel_id", LongType),
    )
  }

  private def getHotelDailyValueSchema = {
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
