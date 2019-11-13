package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 含有状态的wordcount
  */
object WordCountWithState {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCountWithState")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //设置检查点位置
    ssc.checkpoint("./ck")

    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

    val pairs: DStream[(String, Int)] = inputStream.flatMap(_.split(" ")).map((_,1))

    val stateDstream: DStream[(String, Int)] = pairs.updateStateByKey[Int](updateFunc _)

    stateDstream.print()

    ssc.start()

    ssc.awaitTermination()
  }

  /**
    * 定义更新状态方法,用于键值对形式的DStream
    * @param values 当前批次单词频度
    * @param state  为以往批次单词频度
    */
  def updateFunc(values:Seq[Int],state:Option[Int]) ={
    val currentValue: Int = values.foldLeft(0)(_+_)
    val previous: Int = state.getOrElse(0)
    Some(currentValue + previous)
  }
}
