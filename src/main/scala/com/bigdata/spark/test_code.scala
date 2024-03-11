package com.bigdata.spark
import com.bigdata.spark.WordCount.Perform_Word_Count

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}


object test_code {
  // Định nghĩa case class cho dữ liệu của bạn (ví dụ)
  case class Log_Event(LogUserIDOTT: String, playing_session: String, EventType: String, EventCategory : String,
                       LeagueSeasonNameIPTV: String, LeagueRoundNameIPTV: String, Platform: String, PlatformGroup: String, EventIDOTT: String, EventTitle: String,
                       ChannelNoOTT: String, ChannelName: String, ChannelGroup: String, RealTimePlaying: String, device_id: String,
                       SubCompanyNameVN: String, LocationNameVN: String)

  def main(args: Array[String]): Unit = {
    // Cấu hình Kafka Producer
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Khởi tạo Kafka Producer
    val producer = new KafkaProducer[String, String](props)

    // Thông tin topic và message
    val topic = "test"
    val key = "key1"

    // Khởi tạo SparkSession
    val spark = SparkSession.builder().appName("FPT_log").master("local[*]").getOrCreate()

    // Load dữ liệu từ CSV
    import spark.implicits._
    val message: Dataset[Log_Event] = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/event.csv") //event_20240119
      .as[Log_Event]

    val result = message.filter(col("LogUserIDOTT").isNotNull and col("Platform") =!= "box iptv")

    // Biến trạng thái để theo dõi vị trí cần gửi tiếp theo
    var currentPosition = 0

    // Hàm gửi message
    def sendMessage(): Unit = {
      // Chọn dòng mới từ Dataset theo vị trí
      val jsonString = result
        .select(to_json(struct("*")).alias("json_data"))
        .as[String]
        .collect()(currentPosition)

      val record = new ProducerRecord[String, String](topic, key, jsonString)
      producer.send(record)
      println(s"Sent message: $jsonString to topic: $topic with key: $key")

      // Cập nhật vị trí cần gửi tiếp theo
      currentPosition += 1
      if (currentPosition >= message.count()) {
        currentPosition = 0  // Reset về đầu nếu đã gửi hết dữ liệu
      }
    }

    // Hàm schedule gửi message mỗi 10 phút
    def scheduleSendMessage(interval: FiniteDuration): Future[Unit] = {
      Future {
        while (true) {
          sendMessage()
          Thread.sleep(interval.toMillis)
        }
      }
    }

    // Gửi message mỗi 10 phút
    val interval = 1.seconds //seconds
    val sendMessageFuture = scheduleSendMessage(interval)

    // Chờ để giữ chương trình chạy
    Await.result(sendMessageFuture, Duration.Inf)

    // Đóng Kafka Producer sau khi sử dụng
    producer.close()

    // Đóng SparkSession
    spark.stop()
  }
}
