package com.bigdata.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

//sử dụng data type là dataframe
object ReadLog_Channel {

  // Create case class with schema Log
  case class Log_Channel(UserInfo: String,profile_id: String,ItemId: String,device_id: String,type_device: String, PlayingSession: String,
                 Duration: String,LogUserIDOTT: String,Contract: String,TypeRegister: String,ContractType: String,SubType: String,
                 ChannelNoOTT: String,ChannelNoIPTV: String,ChannelName: String,Date: String,View: String,contract_id: String,
                 Zone: String,Area: String,name: String,PlatformGroup: String,Platform: String
                )

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("FPT_log_channel")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "")
      .csv("data/channel_20240219.csv")
      .as[Log_Channel]


//    val s1 = ds.select("ContractType","SubType","ChannelNoOTT","ChannelNoIPTV","ChannelName","Zone","Area","name").filter(col("Platform") =!= "iptv")
//    //.filter(col("platform") === "0")
//    s1.show(s1.count().toInt)

    val result = ds.filter(col("Platform") =!= "iptv").groupBy("LogUserIDOTT")
      .agg(
        count("LogUserIDOTT").alias("number"),
        collect_list("UserInfo").alias("UserInfo"),
        collect_list("profile_id").alias("profile_id"),
        collect_list("ItemId").alias("ItemId"),
        collect_list("device_id").alias("device_id"),
        collect_list("Platform").alias("Platform"),
      )


    // Hiển thị kết quả
    result.show(result.count().toInt)

  }
}
