package com.atguigu.trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSample {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestSample")
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.parallelize(1 to 10)
    val rdd1: RDD[Int] = rdd.sample(true,0.5,2)
    val rdd2: RDD[Int] = rdd.sample(false,0.5)
    val rdd3: RDD[Int] = rdd.sample(false,0.5,2)
    rdd1.collect().foreach(println)
    println("--------------------")
    rdd2.collect().foreach(println)
    println("--------------------")
    rdd3.collect().foreach(println)

    sc.stop()

  }
}
