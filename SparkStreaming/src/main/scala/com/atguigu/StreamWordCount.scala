package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)

    val wordAndCountDStreams: DStream[(String, Int)] = lineStreams.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)

    wordAndCountDStreams.print()

    ssc.start()

    ssc.awaitTermination()
  }
}
