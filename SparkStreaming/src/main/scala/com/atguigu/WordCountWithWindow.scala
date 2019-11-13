package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 带有window操作的wordcount
  */
object WordCountWithWindow {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountWithState")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    ssc.checkpoint("./ck")

    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val pairs: DStream[(String, Int)] = lineStream.flatMap(_.split(" ")).map((_,1))

    val wordAndCountWithWindow: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((x:Int,y:Int)=>{x+y},Seconds(6),Seconds(3))

    wordAndCountWithWindow.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
