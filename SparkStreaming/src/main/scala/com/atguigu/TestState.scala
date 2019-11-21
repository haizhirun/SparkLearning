package com.atguigu

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用来比较mapWithState和updateStateByKey
  */
object TestState {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestState")
    val ssc = new StreamingContext(conf,Seconds(5))

    ssc.checkpoint("./ck3")

    //val input: DStream[String] = ssc.textFileStream("D:\\input.txt")

    val input: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",8888)

    val pairs: DStream[(String, Int)] = input.flatMap(_.split(" ")).map((_,1))

    //1.使用updateStateBykey
    /*
      * update with new values = 1,1
      * (flink,2)
      * update with new values =
      * update with new values = 1,1
      * (flink,2)
      * (hadoop,2)
      * update with new values =
      * (flink,2)
      * update with new values =
      * (hadoop,2)
      * update with new values =
      * (hadoop,2)
      * update with new values =
      * (flink,2)
      * update with new values =
      * update with new values =
      * (flink,2)
      * (hadoop,2)
      */
    pairs.updateStateByKey[Int](undateStateByKeyFunction _).foreachRDD(rdd=>
      rdd.foreach(println)
    )

    println("===========================================================\n\n\n")
    //2.使用mapWithState
    /*
      * update new value with Some(1)
      * update new value with Some(1)
      * (flink,1)
      * (flink,2)
      * update new value with Some(1)
      * update new value with Some(1)
      * (hadoop,1)
      * (hadoop,2)
     */
   /* pairs.mapWithState(StateSpec.function(mapWithStateFunction _)).foreachRDD(rdd =>
      rdd.foreach(println)
    )*/

    ssc.start()
    ssc.awaitTermination()
  }


  def undateStateByKeyFunction(newValues:Seq[Int],oldValue:Option[Int]):Some[Int]={
    println(s"update with new values = ${newValues.mkString(",")}")
    Some(newValues.foldLeft(oldValue.getOrElse(0))(_+_))
  }

  def mapWithStateFunction(key:String,value:Option[Int],state:State[Int])={

    val sum = value.getOrElse(0) + state.getOption().getOrElse(0)
    val output = (key,sum)
    state.update(sum)
    println(s"update new value with $value")
    output
  }

}
