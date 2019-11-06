package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object test {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test")

    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val rddQueue = new mutable.Queue[RDD[Int]]()

    ssc.sparkContext.makeRDD(1 to 300,1).foreach(println)
    /*for(i <- 1 to 5){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300 , 10)
    }*/



  }
}
