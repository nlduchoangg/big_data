package com.bigdata.spark

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


//sử dụng data type là dataframe
object ReadLog_VOD {

  // Create case class with schema Log_VOD
  case class Log_VOD(Date: String,user_id: String,device_id: String,ObjectIDIPTV: String,Contract: String,VodIDIPTV: String,
                 VodIDOTT: String,Title: String,ShowTime: String,Category: String,SubCategory: String,SourceVOD: String,
                 ContractType: String,TypeRegister: String,Zone: String,Area: String,Chipset: String,PlatformName: String,
                 PlatformSubGroup: String,PlatformGroup: String,platform: String,TotalView: String,TotalDevice: String,
                 TotalUser: String,TotalDuration: String
                )

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("FPT_log_vod")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/vod_20240219.csv")
      .as[Log_VOD]

    val result = ds.filter(col("PlatformSubGroup") =!= "Box Linux").groupBy("user_id")
      .agg(
        count("user_id").alias("number"),
        collect_list("device_id").alias("Device_id"),
        collect_list("ObjectIDIPTV").alias("ObjectIDIPTV"),
        collect_list("Contract").alias("Contract"),
        collect_list("Zone").alias("Zone"),
        collect_list("Area").alias("Area"),
      )

    // Hiển thị kết quả
    result.show(result.count().toInt)
  }
}