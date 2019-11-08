package com.atguigu.trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestAction {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestAction")
    val sc = new SparkContext(conf)

    /*al rdd: RDD[Int] = sc.makeRDD(1 to 10, 2)

    val result2: Int = rdd.reduce((x,y)=>(x+y))
    println(result2)

    val rdd2 = sc.makeRDD(Array(("a",1),("a",3),("c",3),("d",5)))
    val result: (String, Int) = rdd2.reduce {
      (x, y) => {
        (x._1 + y._1, x._2 + y._2)
      }
    }
    println(result)

    val rdd3 = sc.parallelize(1 to 10)

    rdd3.foreach{
      (x)=>{
        print(x + "\t")
      }
    }*/

    val nopar = sc.parallelize(List((1,3),(1,2),(2,4),(2,3),(3,6),(3,8)),8)

    nopar.mapPartitionsWithIndex((index,iter)=>{
      Iterator(index.toString + " " + iter.mkString("|"))
    }).collect().foreach(println(_))

    val hashpar = nopar.partitionBy(new org.apache.spark.HashPartitioner(7))
    println(hashpar.count())


  }

}
