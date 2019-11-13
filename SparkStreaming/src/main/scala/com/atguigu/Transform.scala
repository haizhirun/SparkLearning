package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Transform")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //创建DStream
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    //转换为RDD操作,便于操作
    val wordAndCountDStream: DStream[(String, Int)] = inputStream.transform(rdd => {
      val wordAndCountRdd: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
      wordAndCountRdd
    })
    wordAndCountDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
