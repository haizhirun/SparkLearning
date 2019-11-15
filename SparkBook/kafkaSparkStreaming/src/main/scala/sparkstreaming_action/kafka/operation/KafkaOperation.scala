package sparkstreaming_action.kafka.operation

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaOperation {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("KafkaOperation")

    val ssc = new StreamingContext(sparkConf,Seconds(2))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "hadoop102:9092,hadoop103:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafkaOperationGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> false
    )



    kafkaParams

    sparkConf



  }
}
