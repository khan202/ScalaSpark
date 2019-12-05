package DDSpark

import org.apache.spark.sql.functions.{col, unix_timestamp}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * Created by dan.dixey on 18/06/2017.
  */
object Utils {

  /** Wrapper to read data from a .tsv file
    *
    * @param fileName   Path to file
    * @param header     Boolean to include / not include the header
    * @param spark      SparkSession
    * @return
    */
  def readTSV(fileName: String, header: Boolean)(
      implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("sep", "\t") // .tsv
      .option("header", value = header) // No header
      .csv(fileName)
  }

  /** Write a DataFrame to .csv file
    *
    * @param data   DataFrame of data
    * @param path   Filepath to save the data at
    * @param mode   overwrite / append. See the Spark Docs for more optionss
    */
  def writeOne(data: DataFrame,
               path: String,
               mode: String = "overwrite"): Unit = {
    data
      .coalesce(1)
      .write
      .mode(mode)
      .csv(path)
  }

  /** Convert timestamp string to value
    *
    * @param name Name of the column to parse
    * @return     Data in Long format
    */
  private def timeStampConversion(name: String): Column =
    unix_timestamp(col(name), "yyyy-MM-dd'T'HH:mm:ss'Z'")
      .cast(LongType)

  /** Convert a String date to long form
    *
    * @param data   DataFrame of data
    * @param name   Name of the column to convert
    * @return
    */
  def stringDate(data: DataFrame,
                 name: String,
                 existingName: String): DataFrame = {
    data
      .withColumn(name, timeStampConversion(existingName))
      .orderBy(col(name))
  }

  // userid \t timestamp \t musicbrainz-artist-id \t artist-name \t musicbrainz-track-id \t track-name
  val schemaTransactions: StructType = new StructType()
    .add(StructField("userID", StringType, nullable = true))
    .add(StructField("timestamp", StringType, nullable = true))
    .add(StructField("artistId", StringType, nullable = true))
    .add(StructField("artistName", StringType, nullable = true))
    .add(StructField("trackId", StringType, nullable = true))
    .add(StructField("trackName", StringType, nullable = true))
}
