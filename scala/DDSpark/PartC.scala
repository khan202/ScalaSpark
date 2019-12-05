package DDSpark

import DDSpark.Tasks.runPartC
import DDSpark.Utils.{readTSV, writeOne}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * PART C:   Say we define a user’s “session” of Last.fm usage to be comprised of one or
  *           more songs played by that user, where each song is started within 20 minutes
  *           of the previous song’s start time. Create a list of the top 10 longest
  *           sessions, with the following information about each session: userid, timestamp
  *           of first and last songs in the session, and the list of songs played in
  *           the session (in order of play).
  */
object PartC {

  def main(args: Array[String]): Unit = {

    // Load the data from the
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
      conf.getString("DATA_DIR") + conf.getString("transActions"),
      header = false)

    // Run Part C
    val durationMinutes = 20 // Could pass this as an argument
    val output = runPartC(durationMinutes, data)

    // Show the top users
    // output.select("userID").distinct().show()
    /**
      * Four users for the top 10 longest session included
      * sessionID and sessionDuration to provide the distinction between the different
      * sessions.
      */
    // Final Output
    // userID, sessionID, sessionDuration, timestamp (first and last), trackNames (all)
    writeOne(output, conf.getString("DATA_DIR") + conf.getString("OUTPUT_C"))

    // Shutdown Spark
    spark.sparkContext.stop()
  }
}
