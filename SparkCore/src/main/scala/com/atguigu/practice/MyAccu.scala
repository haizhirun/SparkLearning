package com.atguigu.practice

import org.apache.spark.util.AccumulatorV2

/**
  * 实现一个自定义的数值累加器
  */
//abstract class AccumulatorV2[IN, OUT] extends Serializable
class MyAccu extends AccumulatorV2[Int,Int]{

  var sum = 0

  //判断是否为空
  override def isZero: Boolean = {
    sum == 0
  }
 //复制
  override def copy(): AccumulatorV2[Int, Int] = {
    var accu = new MyAccu()
    accu.sum = this.sum
    accu
  }
//重置
  override def reset(): Unit = {
    sum = 0
  }
//累加
  override def add(v: Int): Unit = {
    sum += v
  }

  //合并
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    sum += other.value
  }

  //返回值
  override def value: Int = sum
}
