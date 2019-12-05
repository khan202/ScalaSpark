package DDSpark

import DDSpark.Tasks.runPartA
import DDSpark.Utils.{readTSV, writeOne}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * PART A: Create a list of user IDs, along with the number of distinct songs each user has played.
  */
object PartA {

  def main(args: Array[String]) {

    // Data Located as per the Application.conf file
    val conf = ConfigFactory.load("application.conf")

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

    // Run the task Function
    val output = runPartA(data)

    // Write Output
    writeOne(output, conf.getString("DATA_DIR") + conf.getString("OUTPUT_A"))

    // Shutdown Spark
    spark.sparkContext.stop()

  }
}
