package DDSpark

import DDSpark.Utils.stringDate
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by dan.dixey on 18/06/2017.
  */
object Tasks {

  /** Function to Calculate Part A
    *
    * @param data   Input data
    * @param spark  SparkSession
    * @return
    */
  def runPartA(data: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // userid \t timestamp \t musicbrainz-artist-id \t artist-name \t musicbrainz-track-id \t track-name
    val newNames = Seq("userID",
                       "timestamp",
                       "artistId",
                       "artistName",
                       "trackId",
                       "trackName")
    val distinctDataUpdated = data.toDF(newNames: _*)

    // Register the DataFrame as a global temporary view
    distinctDataUpdated
      .select("userID", "trackId") // using trackID as the name may have change over time
      .distinct()
      .createGlobalTempView("distinctUserPlays")

    // Get the distinct count
    val sqlQuery =
      "SELECT userID, COUNT(userID) AS distinct_songs " +
        "FROM global_temp.distinctUserPlays " +
        "GROUP BY userID " +
        "ORDER BY distinct_songs desc"

    // Get the output
    spark
      .sql(sqlQuery)
  }

  /** Function to Calculate Part B
    *
    * @param data   Input data
    * @param spark  SparkSession
    * @return
    */
  def runPartB(data: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // userid \t timestamp \t musicbrainz-artist-id \t artist-name \t musicbrainz-track-id \t track-name
    val newNames = Seq("userID",
                       "timestamp",
                       "artistId",
                       "artistName",
                       "trackId",
                       "trackName")
    val distinctDataUpdated = data.toDF(newNames: _*)

    // Register the DataFrame as a global temporary view
    distinctDataUpdated
      .filter("artistName is not null")
      .filter("trackName is not null")
      .createGlobalTempView("distinctUserPlays2")

    // Get the distinct count
    val sqlQuery =
      "SELECT artistName, trackName, COUNT(*) AS PlayCount " +
        "FROM global_temp.distinctUserPlays2 " +
        "GROUP BY artistName, trackName " +
        "ORDER BY PlayCount desc"

    // Write the output
    spark
      .sql(sqlQuery)
      .limit(100)
  }

  /** Function to Calculate Part C
    *
    * @param duration   Window size (minutes)
    * @param data       Input data
    * @param spark      SparkSession
    * @return
    */
  def runPartC(duration: Int, data: DataFrame)(
      implicit spark: SparkSession): DataFrame = {

    // userid \t timestamp \t musicbrainz-artist-id \t artist-name \t musicbrainz-track-id \t track-name
    val newNames = Seq("userID",
                       "timestamp",
                       "artistId",
                       "artistName",
                       "trackId",
                       "trackName")
    // Convert date time
    val datetimeData =
      stringDate(data.toDF(newNames: _*), "timeStampParse", "timestamp")

    /**
      * High-level steps:
      *
      *     Phase 1 - preparing the dataframe
      *         1.  Create a new lagged column
      *         2.  Check the time difference between the lagged column is less than 20mins
      *         3.  Create a new Unique Id for this task
      *     Phase 2 - getting the top 10
      *         4.  GroupBy the new Unique Id
      *         5.  Aggregate to find the min and max times
      *         6.  Find the differences
      *         7.  OrderBy length and then take the top 10
      *     Phase 3 - preparing the output
      *         8.  Requires:
      *               userid, timestamp (first and last), trackNames (all)
      *         9.  Left join Phase 2 with Phase 1
      *         10. Select columns for writing back to disk as .csv
      */
    // Params
    val minutesToSeconds = duration * 60

    // Windowing functionality
    val windowingPartition =
      Window.partitionBy("userID").orderBy("timeStampParse")

    // Calculated operations
    val isDifference = col("timeStampParse").cast(LongType) - lag(
      col("timeStampParse").cast(LongType),
      1,
      0)
      .over(windowingPartition) >= minutesToSeconds
    val uniqueSession =
      concat(col("userID"), sum("isDifference").over(windowingPartition))

    // Phase 1
    val phaseOne = datetimeData
      .withColumn("isDifference", isDifference.cast(IntegerType))
      .withColumn("uniqueSessionID", uniqueSession)
      .persist()

    // Phase 2
    val phaseTwo = phaseOne
      .groupBy("uniqueSessionID")
      .agg(min("timeStampParse").alias("firstSongT"),
           max("timeStampParse").alias("lastSongT"))
      .withColumn(
        "sessionDuration",
        col("lastSongT").cast(LongType) - col("firstSongT").cast(LongType))
      .orderBy(col("sessionDuration").desc)
      .limit(10) // top 10 longest

    // Show the top 10 longest session IDs
    // phaseTwo.show(10)

    // Phase 3
    phaseTwo
      .join(phaseOne, "uniqueSessionID")
      .select("userID",
              "uniqueSessionID",
              "sessionDuration",
              "firstSongT",
              "lastSongT",
              "trackName")
  }
}
