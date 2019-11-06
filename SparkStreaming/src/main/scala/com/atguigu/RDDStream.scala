package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDStream {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

    val ssc = new StreamingContext(conf,Seconds(5))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue)


    val reduceStream = inputStream.reduce(_+_)

    reduceStream.print()

    ssc.start()

    for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 100 , 10)
      Thread.sleep(2000)
    }


    ssc.awaitTermination()
  }
}
