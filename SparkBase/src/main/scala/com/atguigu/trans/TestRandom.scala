package com.atguigu.trans

import scala.util.Random

object TestRandom {
  def main(args: Array[String]): Unit = {
    val r1 = new Random(1000)
    val r2 = new Random(1000)

    //随机数种子固定之后，随机数也变成了固定的一些数字
    for(i <- 1 to 10){
      println(r1.nextInt(10))
    }
    println("-----------")
    for(j <- 1 to 10){
      println(r2.nextInt(10))
    }
  }
}
