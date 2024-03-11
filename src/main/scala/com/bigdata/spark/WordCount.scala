package com.bigdata.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{collect_list, _}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

//sử dụng data type là dataframe
object WordCount {

  //case clas log file dung dua
  case class Log_Event(LogUserIDOTT: String, playing_session: String, EventType: String, EventCategory : String,
                       LeagueSeasonNameIPTV: String, LeagueRoundNameIPTV: String, Platform: String, PlatformGroup: String, EventIDOTT: String, EventTitle: String,
                       ChannelNoOTT: String, ChannelName: String, ChannelGroup: String, RealTimePlaying: String, device_id: String,
                       SubCompanyNameVN: String, LocationNameVN: String)


  /** Our main function where the action happens */
  def main(args: Array[String]) {
    Perform_Word_Count()
    }
  def Perform_Word_Count(): Dataset[Log_Event] = {

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
      .csv("data/event.csv") //event_20240119
      .as[Log_Event]

    val result = ds.filter(col("LogUserIDOTT").isNotNull and col("Platform") =!= "box iptv")
    //print(result.show(result.count().toInt))
    //print(result.getClass)

    //Chuyển đổi Dataset thành định dạng JSON
//    val jsonStringDataset = result
//      .select(to_json(struct("*")).alias("json_data"))
//      .as[String]

    //jsonStringDataset.show(jsonStringDataset.count().toInt, truncate = false)

    //print(jsonStringDataset.getClass)
    // Đóng SparkSession
    return result
    //spark.stop()

  }
}



