//package com.bigdata.spark
//
//import org.apache.log4j._
//import org.apache.spark._
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
//
//object ReadLog {
//
//  def parseLine(line: String): (String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String) = {
//    // Split by commas
//    val fields = line.split(",")
//    // Extract the age and numFriends fields, and convert to integers
//    val stt = fields(0).toString
//    val Contract = fields(1).toString
//    val LogUserIDOTT = fields(2).toString
//    val EventIDIPTV = fields(3).toString
//    val EventIDOTT = fields(4).toString
//    val EventTitle = fields(5).toString
//    val TimeLive = fields(6).toString
//    val EndTime = fields(7).toString
//    val EventCategory = fields(8).toString
//    val EventType = fields(9).toString
//    val League = fields(10).toString
//    val ContentIDIPTV = fields(11).toString
//    val ContentIDOTT = fields(12).toString
//    //val ChannelNoIPTV = fields(13).toString
//    val ChannelNoOTT = fields(14).toString
//    val ChannelName = fields(15).toString
//    val ChannelGroup = fields(16).toString
//    //val TypeRegister = fields(17).toString
//    //val ContractType = fields(18).toString
//    val RealTimePlaying = fields(19).toString
//    val Platform = fields(20).toString
//    val PlatformGroup = fields(21).toString
//    val device_id = fields(22).toString
//    val playing_session = fields(23).toString
//    val SubCompanyNameVN = fields(24).toString
//    val LocationNameVN = fields(25).toString
//
//    // Create a tuple that is our result.
//    (Contract, LogUserIDOTT, EventIDIPTV, EventIDOTT, EventTitle, TimeLive, EndTime, EventCategory, EventType, League, ContentIDIPTV, ContentIDOTT, ChannelNoOTT, ChannelName, ChannelGroup, RealTimePlaying, Platform, PlatformGroup, device_id, playing_session, SubCompanyNameVN, LocationNameVN)
//  }
//
//
//  /** Our main function where the action happens */
//  def main(args: Array[String]) {
//
//    // Set the log level to only print errors
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    // Create a SparkContext using every core of the local machine
//    val sc = new SparkContext("local[*]", "FPT_Log")
//
//    // Read each line of input data
//    val lines = sc.textFile("data/event.csv")
//    val parsedLines = lines.map(parseLine)
//
//
//  }
//}


//package com.bigdata.spark
//
//import org.apache.log4j._
//import org.apache.spark._
//import org.apache.spark.sql.{Encoders, SparkSession}
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
//import org.apache.spark.sql.functions.{collect_list, _}
//import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
//
//import org.apache.spark.sql.{Encoder, Encoders}
//
////sử dụng data type là dataframe
//object WordCount {
//
//  // Create case class with schema Log_Event
//  //case class log file son dua
//  //  case class Log_Event(LogUserIDOTT: String, playing_session: String, EventType: String, EventCategory : String,
//  //                 League: String, Platform: String, PlatformGroup: String, EventIDOTT: String, EventTitle: String,
//  //                 ChannelNoOTT: String, ChannelName: String, ChannelGroup: String, RealTimePlaying: String, device_id: String,
//  //                 SubCompanyNameVN: String, LocationNameVN: String)
//  //case clas log file dung dua
//  case class Log_Event(LogUserIDOTT: String, playing_session: String, EventType: String, EventCategory : String,
//                       LeagueSeasonNameIPTV: String, LeagueRoundNameIPTV: String, Platform: String, PlatformGroup: String, EventIDOTT: String, EventTitle: String,
//                       ChannelNoOTT: String, ChannelName: String, ChannelGroup: String, RealTimePlaying: String, device_id: String,
//                       SubCompanyNameVN: String, LocationNameVN: String)
//
//  /** Our main function where the action happens */
//  def main(args: Array[String]) {
//    Perform_Word_Count()
//    //    val result = ds.filter(col("LogUserIDOTT").isNotNull and col("Platform") =!= "box iptv").groupBy("LogUserIDOTT")
//    //      .agg(
//    //        count("LogUserIDOTT").alias("number"),
//    //        collect_list("LogUserIDOTT").alias("LogUserIDOTT"),
//    //        collect_list("Platform").alias("Platform"),
//    //        collect_list("PlatformGroup").alias("PlatformGroup"),
//    //        collect_list("device_id").alias("device_id")
//    //        //        collect_list("EventCategory").alias("EventCategory"),
//    //        //        collect_list("EventTitle").alias("EventTitle"),
//    //        //        collect_list("LeagueSeasonNameIPTV").alias("leagueSeasonNameIPTV"),
//    //        //        collect_list("LeagueRoundNameIPTV").alias("leagueRoundNameIPTV"),
//    //      )
//    //
//    //    // Hiển thị kết quả
//    //    result.show(result.count().toInt)
//  }
//  def Perform_Word_Count(): Unit = {
//
//    // Set the log level to only print errors
//    Logger.getLogger("org").setLevel(Level.ERROR)
//
//    // Create a SparkSession using every core of the local machine
//    val spark = SparkSession
//      .builder
//      .appName("FPT_log")
//      .master("local[*]")
//      .getOrCreate()
//
//    // Load each line of the source data into an Dataset
//    import spark.implicits._
//    val ds = spark.read
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .csv("data/event.csv") //event_20240119
//      .as[Log_Event]
//
//  }
//}



