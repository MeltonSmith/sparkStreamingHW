package elasticUpload

import biz.paluch.logging.gelf.log4j.GelfLogAppender
import model.HotelState
import org.apache.log4j.spi.{Filter, LoggingEvent}
import org.apache.log4j.varia.{LevelMatchFilter, LevelRangeFilter}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
import utils.Schemas.getHotelStateSchema

/**
 * Created by: Ian_Rakhmatullin
 * Date: 30.04.2021
 */
object AppForElastic {
  def main(args: Array[String]): Unit = {

    val rootLogger = Logger.getRootLogger
    rootLogger.addAppender(createGelfLogAppender(host = "tcp:localhost", port = 5000))

    val spark: SparkSession = SparkSession
      .builder
      .appName("sparkStreamingForElastic")
      .config("spark.es.nodes","localhost") //default
      .config("spark.es.port","9200")
      .config("spark.es.nodes.wan.only","true")
      .getOrCreate()


    import spark.implicits._

    val finalResultStream = spark.readStream
                                  .format("parquet")
                                  .schema(StructType(getHotelStateSchema))
                                  .option("maxFilesPerTrigger", 1)
                                  .load("/201 HW Dataset/finalResultSplit/0")
                                  .drop("batch_timestamp")
                                  .withColumn("batch_timestamp", current_timestamp())
                                  .as[HotelState]

    val query = finalResultStream
                      .writeStream
                      .outputMode("update")
                      .foreachBatch((batchDF: Dataset[HotelState], batchId: Long) => {
                        rootLogger.warn("batch number: " + s"$batchId" + " is being processed")
                        EsSpark.saveToEs(batchDF.toJavaRDD, "hotels/data")
                      })
                      .start()
//                 .format("console")
//                .outputMode("update")
//                .start()

    query.awaitTermination()

  }

  /**
   * Create and configure appender
   * @param host for logstash
   * @param port for logstash
   */
  def createGelfLogAppender(host: String, port: Int): GelfLogAppender = {
    val filter = new LevelRangeFilter()
    filter.setLevelMin(Level.WARN)
    filter.setLevelMin(Level.WARN)
    filter.setAcceptOnMatch(true)

    val appender = new GelfLogAppender()
    appender.setHost(host)
    appender.setPort(port)
    appender.addFilter(filter)
    appender.activateOptions()
    appender

  }
}
