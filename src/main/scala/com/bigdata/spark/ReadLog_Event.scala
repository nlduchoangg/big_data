package com.bigdata.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{collect_list, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

//sử dụng data type là dataframe
object ReadLog_Event {

  case class Log_Event(LogUserIDOTT: String, playing_session: String, EventType: String, EventCategory : String,
                       LeagueSeasonNameIPTV: String, LeagueRoundNameIPTV: String, Platform: String, PlatformGroup: String, EventIDOTT: String, EventTitle: String,
                       ChannelNoOTT: String, ChannelName: String, ChannelGroup: String, RealTimePlaying: String, device_id: String,
                       SubCompanyNameVN: String, LocationNameVN: String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("FPT_log")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/data_event_20240304.csv") //event_20240119
      .as[Log_Event]


    val result = ds.filter(col("LogUserIDOTT").isNotNull and col("Platform") =!= "box iptv").groupBy("LogUserIDOTT")
      .agg(
        count("LogUserIDOTT").alias("number"),
        collect_list("LogUserIDOTT").alias("LogUserIDOTT"),
        collect_list("Platform").alias("Platform"),
        collect_list("PlatformGroup").alias("PlatformGroup"),
        collect_list("device_id").alias("device_id")
//        collect_list("EventCategory").alias("EventCategory"),
//        collect_list("EventTitle").alias("EventTitle"),
//        collect_list("LeagueSeasonNameIPTV").alias("leagueSeasonNameIPTV"),
//        collect_list("LeagueRoundNameIPTV").alias("leagueRoundNameIPTV"),
      )

//    val s1 = ds.filter(col("LogUserIDOTT").isNotNull and col("Platform") =!= "box iptv").select("LogUserIDOTT", "playing_session")
//    s1.show(s1.count().toInt, false)

    // Hiển thị kết quả
    result.show(result.count().toInt)

//     dem xem co bao record ma co platform != box iptv
//    val filteredDs = ds.filter(col("Platform") =!= "box iptv")
//    //sql.show(sql.count().toInt, truncate = false)
//    print(filteredDs.count())
  }
}
