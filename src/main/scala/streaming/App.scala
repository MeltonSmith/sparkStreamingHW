package streaming

import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.concurrent.duration.DurationInt

/**
 * @author ${user.name}
 */
object App {

  def main(args : Array[String]) {
    val spark = SparkSession
      .builder
      .appName("sparkStreamingHW2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

//    val expedia = spark.read
//                      .format("avro")
//                      .load("/201 HW Dataset/expedia")

    import spark.implicits._

    //TODO works
    val frame = spark.readStream
                      .format("avro")
                      .schema(StructType(getExpediaInputSchema))
                      .load("/201 HW Dataset/expedia")

    val query = frame
                  .withColumnRenamed("srch_ci", "value")
                  .writeStream
                  .outputMode("append")
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9094")
                  .option("checkpointLocation", "tmp/dsd")
                  .option("topic", "topic1488")
                  .start()

//    val query = frame
//                .writeStream
//                .outputMode("append")
//                .format("console")
//                .start()



//
//    val hotelsKafka = spark.readStream
//                          .format("kafka")
//                          .option("kafka.bootstrap.servers", "localhost:9094")
//                          .option("subscribe", "daysUnique")
//                          .load()

//    val query = hotelsKafka.selectExpr("CAST(value as STRING) as value")
//                            .as[(String)]
//                            .groupBy("value")
//                            .count()
//                            .writeStream
//                            .outputMode("complete")
//                            .format("console")
//                            .trigger(Trigger.ProcessingTime(10.seconds))
//                            .start()

    query.awaitTermination()






//    spark.readStream
//                      .json(hotelsKafka.selectExpr("CAST(value as STRING) as value")
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

}
