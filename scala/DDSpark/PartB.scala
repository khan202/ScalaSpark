package DDSpark

import DDSpark.Tasks.runPartB
import DDSpark.Utils.{readTSV, writeOne}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * PART B:   Create a list of the 100 most popular songs (artist and title) in the
  *           dataset, with the number of times each was played.
  */
object PartB {

  def main(args: Array[String]) {

    // Data Located as per the Application.conf file
    val conf = ConfigFactory.load()

    // SparkSession
    implicit val spark = SparkSession
      .builder()
      .appName(conf.getString("sparkAppName") + "C")
      .config("spark.executor.memory", conf.getString("sparkExecutorMemory"))
      .config("spark.master", conf.getString("sparkMaster"))
      .config("spark.driver.memory", conf.getString("sparkDriverMemory"))
      .getOrCreate()

    // Read the data in as Spark DataFrame
    val data = readTSV(
      conf.getString("DATA_DIR") + conf.getString("transActions"), header = false)

    val output = runPartB(data)

    // Write Output
    writeOne(output, conf.getString("DATA_DIR") + conf.getString("OUTPUT_B"))

    // Shutdown Spark
    spark.sparkContext.stop()
  }
}
