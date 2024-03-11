import org.apache.spark.sql.{SaveMode, SparkSession, Encoders}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.JavaConverters._

object KafkaConsumer {
  case class Message(message: String)

  def main(args: Array[String]): Unit = {
    // Cấu hình Kafka Consumer
    val kafkaProps = new java.util.Properties()
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    //console-consumer-34952  console-consumer-59861  console-consumer-100
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-75456")
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")


    // Khởi tạo Kafka Consumer
    val consumer = new KafkaConsumer[String, String](kafkaProps)

    // Khởi tạo Spark Session
    val spark = SparkSession.builder
      .appName("KafkaConsumerToHDFS")
      .master("local[*]") // Thêm dòng này để cung cấp master URL
      .getOrCreate()

    try {
      // Đăng ký consumer cho topic "your_topic"
      val topic = "test"
      consumer.subscribe(List(topic).asJava)

      // Đặt consumer ở đầu topic để đọc tất cả message từ đầu
      consumer.seekToBeginning(consumer.assignment())

      // Define schema for the Dataset
      val schema = new StructType().add("message", StringType)

      // Import implicits để sử dụng encoder mặc định
      import spark.implicits._

      // Vòng lặp để nhận và xử lý message từ Kafka
      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(1000))
        records.asScala.foreach { record =>
          val message = record.value()
          println(s"Received message: $message from topic: ${record.topic()} with key: ${record.key()}")

          // Xử lý message bằng Spark và ghi xuống HDFS
          val data = Seq(Message(message))
          val ds = spark.createDataset(data)(Encoders.product[Message])

          // Kiểm tra xem tệp Parquet đã tồn tại hay không
          val parquetFilePath = "hdfs://127.0.0.1:9000/usr/nlduc/Data_HDFS_log"
          val existingData = try {
            spark.read.parquet(parquetFilePath).as[Message]
          } catch {
            case _: org.apache.spark.sql.AnalysisException => {
              // Nếu tệp không tồn tại, tạo một Dataset rỗng
              spark.createDataset(spark.sparkContext.emptyRDD[Message])(Encoders.product[Message])
            }
          }

          // Ghi nối tiếp dữ liệu vào tệp Parquet đã tồn tại
          ds.union(existingData).write.mode(SaveMode.Append).parquet(parquetFilePath)
        }
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      // Đóng Kafka Consumer và Spark sau khi sử dụng
      consumer.close()
      spark.stop()
    }
  }
}
