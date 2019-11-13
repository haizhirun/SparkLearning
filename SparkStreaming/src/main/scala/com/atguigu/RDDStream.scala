package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDStream {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

    val ssc = new StreamingContext(conf,Seconds(2))

    //创建rdd队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue,oneAtATime = false)

    inputStream.map(x => (x%10,1)).reduceByKey(_+_).print()

    //启动任务
    ssc.start()

    //循环创建并向rdd队列中放入rdd
    for(i <- 0 to 10){
      val inputRdd: RDD[Int] = ssc.sparkContext.makeRDD(1 to 100,2)
      rddQueue += inputRdd
      Thread.sleep(2000)
    }


    ssc.awaitTermination()


  }
}
