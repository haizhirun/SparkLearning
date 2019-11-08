package com.atguigu.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MyAccuTest {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyAccuTest")
    //创建SC
    val sc = new SparkContext(sparkConf)

    //创建自定义累加器对象
    val accu = new MyAccu()

    //注册累加器
    sc.register(accu)
    val value: RDD[Int] = sc.parallelize(Array(1,2,3,4))

    //在行动算子中对累加器的值进行修改
    value.foreach(
      x=>{
        accu.add(1)
        println(x)
      }
    )
    println(accu.value)

    sc.stop()
  }
}
