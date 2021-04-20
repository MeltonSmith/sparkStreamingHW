package sessionWrapper

import org.apache.spark.sql.SparkSession

/**
 * Created by: Ian_Rakhmatullin
 * Date: 20.04.2021
 */
trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("testing")
//      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()
  }
}
