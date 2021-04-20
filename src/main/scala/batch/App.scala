package batch

import model.VisitType._
import model.{GroupingKey, HotelState, VisitType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

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
            "Standard stay": 2-7 days
            "Standard extended stay": 1-2 weeks
            "Long stay": 2-4 weeks (less than month)
        Add most_popular_stay_type for a hotel (with max count)
    Store it as initial state (For examples: hotel, batch_timestamp, erroneous_data_cnt, short_stay_cnt, standard_stay_cnt, standard_extended_stay_cnt, long_stay_cnt, most_popular_stay_type).
    In streaming way read Expedia data for 2017 year from HDFS on WSL2. Read initial state, send it via broadcast into streaming. Repeat previous logic on the stream.
    Apply additional variance with filter on children presence in booking (with_children: false - children <= 0; true - children > 0).
    Store final data in HDFS. (Result will look like: hotel, with_children, batch_timestamp, erroneous_data_cnt, short_stay_cnt, standart_stay_cnt, standart_extended_stay_cnt, long_stay_cnt, most_popular_stay_type

 *
 * Created by: Ian_Rakhmatullin
 * Date: 11.04.2021
 */
object App {
  val durationOfStay = "durationOfStay"

  //condition expressions
  val erroneousCondition: Column = col(durationOfStay)
                              .isNull
                              .or(col(durationOfStay).gt(30))
                              .or(col(durationOfStay).leq(0))

  val shortStayCond: Column = col(durationOfStay).equalTo(1)

  val standardStayCond: Column = col(durationOfStay).between(2, 7)
  val standardExtendedStayCond: Column = col(durationOfStay).between(8, 14)
  val longStayCond: Column = col(durationOfStay).between(15, 29)

  //const for fields
  val hotelsWeatherTopic = "hotelDailyDataUnique"
  val firstYear = "2016"
  val secondYear = "2017"

  val withChildren = "withChildren"
  val hotel_id = "hotel_id"
  val erroneous_data_cnt = "erroneous_data_cnt"
  val short_stay_cnt = "short_stay_cnt"
  val standard_stay_cnt = "standard_stay_cnt"
  val standard_extended_stay_cnt = "standard_extended_stay_cnt"
  val long_stay_cnt = "long_stay_cnt"
  val most_popular_stay_type = "most_popular_stay_type"

  //column numbers after the join between 2016 and 2017 data for aggregation with state
  val erroneousDataColumnNumber = 3
  val shortStayColumnNumber = 4
  val standardDataColumnNumber = 5
  val standardExDataColumnNumber = 6
  val longStayDataColumnNumber = 7
  val stayTypeColumnNumber = 8
  val batchTimeStampColumnNumber = 9

  //condition for finding the maximum among the visit types
  val greatestAmongTheCounts: Column = greatest(erroneous_data_cnt, short_stay_cnt, standard_stay_cnt, standard_extended_stay_cnt, long_stay_cnt)

  /**
   * Updates state for the group of hotel_id + children presence
   */
  def updateFunction(groupingKey: GroupingKey,
                     inputs: Iterator[Row],
                     oldState: GroupState[HotelState]):HotelState = {
    val state: HotelState =
      if (oldState.exists) oldState.get
      else {
        //in case of left join the following fields may be nulls
        val row = inputs.next()
        val err_cnt = getCount(row, erroneousDataColumnNumber)
        val short_cnt = getCount(row, shortStayColumnNumber)
        val standard_cnt = getCount(row, standardDataColumnNumber)
        val standard_ex_cnt = getCount(row, standardExDataColumnNumber)
        val long_stay_cnt = getCount(row, longStayDataColumnNumber)

        val stayType = VisitType.withName(Option(row.getAs[String](stayTypeColumnNumber)).getOrElse(erroneousStr))
        val timeStamp = row.getTimestamp(batchTimeStampColumnNumber) //can't be null as we specified it explicitly

        val justCreatedState = HotelState(groupingKey.hotel_id, groupingKey.withChildren, timeStamp, err_cnt, short_cnt, standard_cnt, standard_ex_cnt, long_stay_cnt, stayType)
        val visitType: VisitType.Value = getVisitTypeFromRow(row)
        justCreatedState.updateState(visitType)
      }

    for (input <- inputs) {
      val visitType: VisitType.Value = getVisitTypeFromRow(input)
      state.updateState(visitType)
    }
    oldState.setTimeoutTimestamp(state.batch_timestamp.getTime + 2000)
    state
  }


  def main(args : Array[String]) {
    val spark = SparkSession
                    .builder
                    .appName("sparkStreamingHW2")
                    .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._


    ///2016 as a static batch///
    val expedia2016 = spark.read
                      .format("avro")
                      .schema(StructType(getExpediaInputSchema))
                      .load("/201 HW Dataset/expedia")
                      .withColumn(withChildren,
                                    when(col("srch_children_cnt").gt(0), true)
                                    .otherwise(false))
                      .withColumn(durationOfStay, expr("DATEDIFF((CAST(srch_co AS DATE)), (CAST(srch_ci AS DATE)))"))
                      .withColumn("key", concat(col(hotel_id),
                          lit("/"),
                          col("srch_ci")))
                      .where("year(CAST(srch_ci AS DATE)) == " + firstYear)

    val hotelDailyKafka = spark
                          .read
                          .format("kafka")
                          .option("kafka.bootstrap.servers", "localhost:9094")
                          .option("startingOffsets", "earliest")
                          .option("subscribe", hotelsWeatherTopic)
                          .load()
                          .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
                          .withColumn("jsonData", from_json(col("value"), StructType(getHotelDailyValueSchema))).as("data")
                          .select("key", "jsonData.*")
                          .where(col("avg_tmpr_c").isNotNull
                                .and
                                (col("avg_tmpr_c").gt(0))
                          )

    hotelDailyKafka.persist(StorageLevel.MEMORY_ONLY)

    //enriching with weather
    val joinResult = expedia2016.as("exp")
            .join(hotelDailyKafka.as("hotelDaily"),
              $"hotelDaily.key" === $"exp.key"
            )
            .select("exp.*", "hotelDaily.avg_tmpr_c")



    val result2016 = joinResult
                  .groupBy(hotel_id, withChildren)
                  .agg(
                    count(when(erroneousCondition, true)).cast(IntegerType).as(erroneous_data_cnt),
                    count(when(shortStayCond, true)).cast(IntegerType).as(short_stay_cnt),
                    count(when(standardStayCond, true)).cast(IntegerType).as(standard_stay_cnt),
                    count(when(standardExtendedStayCond, true)).cast(IntegerType).as(standard_extended_stay_cnt),
                    count(when(longStayCond, true)).cast(IntegerType).as(long_stay_cnt)
                  )
                  .withColumn(most_popular_stay_type,
                    when(greatestAmongTheCounts === short_stay_cnt, shortStayStr)
                      .when(greatestAmongTheCounts === standard_stay_cnt, standardStr)
                      .when(greatestAmongTheCounts === standard_extended_stay_cnt, standardExStr)
                      .when(greatestAmongTheCounts === long_stay_cnt, longStr)
                      .otherwise(erroneousStr)
                  )


    ////2017/////
    val expedia2017Stream = spark.readStream
                              .format("avro")
                              .schema(StructType(getExpediaInputSchema))
                              .load("/201 HW Dataset/expedia")
                              .withColumn(withChildren,
                                              when(col("srch_children_cnt").gt(0), true)
                                              .otherwise(false))
                              .withColumn(durationOfStay, expr("DATEDIFF((CAST(srch_co AS DATE)), (CAST(srch_ci AS DATE)))"))
                              .withColumn("key", concat(col(hotel_id),
                                lit("/"),
                                col("srch_ci")))
                              .where("year(CAST(srch_ci AS DATE)) == " + secondYear)

    //joining with expedia for 2017
    val joinResult2017 = expedia2017Stream.as("exp")
                                      .join(hotelDailyKafka.as("hotelDaily"),
                                        $"hotelDaily.key" === $"exp.key"
                                      )
                                      .select("exp.*", "hotelDaily.avg_tmpr_c")


    //final result - join between 2017 and static broadcast 2016
    val query = joinResult2017.as("r2017")
                .join(broadcast(result2016).as("r2016"),
                  Seq(hotel_id, withChildren),
                  "left_outer"
                  )
                .withColumn("batch_timestamp", current_timestamp())
                .withWatermark("batch_timestamp", "0 milliseconds")
                .select("r2017." + hotel_id,
                  "r2017."+ withChildren,
                  "r2017." + durationOfStay,
                  "r2016." + erroneous_data_cnt,
                  "r2016." + short_stay_cnt,
                  "r2016." + standard_stay_cnt,
                  "r2016." + standard_extended_stay_cnt,
                  "r2016." + long_stay_cnt,
                  "r2016." + most_popular_stay_type,
                  "batch_timestamp")
                .groupByKey(row => GroupingKey(row.getAs[Long](0), row.getAs[Boolean](1)))
                .mapGroupsWithState(GroupStateTimeout.EventTimeTimeout())(updateFunction)
//                .writeStream
//                .outputMode("update")
//                .foreachBatch((batchDF: Dataset[HotelState], batchId: Long) =>
//                  if (!batchDF.isEmpty){
//                    batchDF
//                      .repartition(1) //due to small amount of the data
//                      .write
//                      .format("parquet")
//                      .save(s"/201 HW Dataset/finalResult/$batchId")
//                  }
//                )
//                .start()

                              .writeStream
                              .format("console")
                              .outputMode("update")
                              .start()

    query.awaitTermination()
  }


  /**
   * Util
   * @return extracted count from a row by a specified column position
   */
  private def getCount(row: Row, columnNumber: Int) = {
    Option(row.get(columnNumber)).getOrElse(0).asInstanceOf[Int]
  }

  /**
   * Util
   * @return extracted visit type from a row
   */
  private def getVisitTypeFromRow(row: Row) = {
    val duration = Option(row.get(2)).getOrElse(-1).asInstanceOf[Int]
    defineVisitType(duration)
  }

  private def getExpediaInputSchema = {
    List(
      StructField("id", LongType),
      StructField("srch_ci", StringType),
      StructField("srch_children_cnt", IntegerType),
      StructField("srch_co", StringType),
      StructField(hotel_id, LongType),
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
