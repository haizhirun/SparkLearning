package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val lineStreams: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)

    val wordStreams: DStream[String] = lineStreams.flatMap(_.split(" "))

    val wordAndOneStreams: DStream[(String, Int)] = wordStreams.map((_,1))

    val wordAndCountStreams: DStream[(String, Int)] = wordAndOneStreams.reduceByKey(_+_)

    wordAndCountStreams.print()

    ssc.start()

    ssc.awaitTermination()
  }


}
