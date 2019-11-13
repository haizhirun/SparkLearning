package com.atguigu

import org.apache.hadoop.hdfs.protocol.datatransfer.Sender
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用自定义的数据源采集数据
  */
object CustomerReceiverTest {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("CustomerReceiverTest")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val inputStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102",9999))

    inputStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()

    ssc.awaitTermination()
  }
}
