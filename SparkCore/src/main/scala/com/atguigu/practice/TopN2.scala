package com.atguigu.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1. 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
  *
  * 样本如下：
  * 1516609143867 6 7 64 16
  * 1516609143869 9 4 75 18
  * 1516609143869 1 7 87 12
  * 2. 需求：统计出每一个省份广告被点击次数的TOP3
  *
  * 思路如下：
  * 1.读取数据生成RDD：TS，Province，City，User，AD
  * 2.按照最小粒度聚合：((Province,AD),1)
  * 3.计算每个省中每个广告被点击的总数：((Province,AD),sum)
  * 4.将省份作为key,广告加点击数做为value:(Province,(AD,sum))
  * 5.将同一个省份的所有广告进行聚合(Province,List((Ad1,sum1),(Ad2,sum2)))
  * 6.将同一个省份所有的广告的集合进行排序，并取出前三条
  */
object TopN2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Top2")
    val sc = new SparkContext(conf)

    val line: RDD[String] = sc.textFile("D:\\workspace\\ideaWorkSpace\\hust\\bigdata\\Spark-Learning\\SparkCore\\src\\main\\resources\\agent.log")

    //1.提取出需要数据，形成元组:((province,ad),1)
    val provinceAdAndOne: RDD[((String, String), Int)] = line.map {
      x => {
        val fields: Array[String] = x.split(" ")
        ((fields(1), fields(4)), 1)
      }
    }
    //2.计算出每个省份每个广告点击总数：((province,ad),sum)
    val provinceAdAndSum: RDD[((String, String), Int)] = provinceAdAndOne.reduceByKey((x,y)=>(x+y))

    //3.将province作为key,ad和sum作为value:(province,(ad,sum))
    val provinceAndAdSum: RDD[(String, (String, Int))] = provinceAdAndSum.map {
      x => {
        (x._1._1, (x._1._2, x._2))
      }
    }
    //4.将同一省份的广告进行聚集
    val provinceAndAdSumAgg: RDD[(String, Iterable[(String, Int)])] = provinceAndAdSum.groupByKey()

    //5.将同一省份的广告按照点击数进行逆序排序，然后取出前三，首先将迭代器转换为List集合
    val provinceAndAdSumTop3: RDD[(String, List[(String, Int)])] = provinceAndAdSumAgg.mapValues {
      x => {
        x.toList.sortWith(_._2 > _._2).take(3)
      }
    }
    provinceAndAdSumTop3.collect().foreach(println)

    sc.stop()
  }


}
