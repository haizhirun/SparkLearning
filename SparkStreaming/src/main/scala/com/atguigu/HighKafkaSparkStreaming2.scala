package com.atguigu

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HighKafkaSparkStreaming2 {

  def main(args: Array[String]): Unit = {

    //获取SSC
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1",()=>createSSC())


    ssc.start()
    ssc.awaitTermination()
  }

  def createSSC(): StreamingContext = {
    //创建配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafkaSparkStreaming2")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("./ck1")

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

    //读取kafka数据创建Dstream
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaPara,Set(topic))

    //打印
    kafkaStream.print()

    ssc
  }



}
