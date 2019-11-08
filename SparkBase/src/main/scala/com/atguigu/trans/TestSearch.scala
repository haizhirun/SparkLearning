package com.atguigu.trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSearch {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("TestSearch").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val word: RDD[String] = sc.makeRDD(Array("a","b","c","d"))

    val search = new Search("a")

    val filtered: RDD[String] = search.getMatch1(word)

    filtered.foreach(println)

    sc.stop()
  }
}
