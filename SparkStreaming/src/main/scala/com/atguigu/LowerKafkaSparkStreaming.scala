package com.atguigu

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka}
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream

object LowerKafkaSparkStreaming {


  def main(args: Array[String]): Unit = {
    //创建配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LowerKafkaSparkStreaming")

    val ssc = new StreamingContext(sparkConf,Seconds(3))


    val brokers:String = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic : String = "first"
    val group : String = "spark2kafka"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //封装kafka
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    //获取当前消费的offset
    val fromOffset:Map[TopicAndPartition,Long] = getOffSet()
    //消费kafka数据创建DStream
    val kafkaDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaPara,
      fromOffset,
      message => message.message()
    )

    kafkaDstream.print()

    setOffset()

    ssc.start()
    ssc.awaitTermination()
  }

  //获取offset
  def getOffSet(): Map[TopicAndPartition, Long] = ???

  //保存offseet
  def setOffset() = ???
}
