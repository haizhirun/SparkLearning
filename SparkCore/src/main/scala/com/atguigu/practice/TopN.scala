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
  *
  *
  * 思路如下：
  * 1.读取数据生成RDD：TS，Province，City，User，AD
  * 2.按照最小粒度聚合：((Province,AD),1)
  * 3.计算每个省中每个广告被点击的总数：((Province,AD),sum)
  * 4.将省份作为key,广告加点击数做为value:(Province,(AD,sum))
  * 5.将同一个省份的所有广告进行聚合(Province,List((Ad1,sum1),(Ad2,sum2)))
  * 6.将同一个省份所有的广告的集合进行排序，并取出前三条
  */
object TopN {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TopN")
    val sc = new SparkContext(conf)

    //读取数据生成的RDD:timestamp,province,city,user,ad
    val line: RDD[String] = sc.textFile("D:\\workspace\\ideaWorkSpace\\hust\\bigdata\\Spark-Learning\\SparkCore\\src\\main\\resources\\agent.log")

    //按照最小粒度聚合((province,ad),1)
    val provinceAndAdOne: RDD[((String, String), Int)] = line.map(
      x => {
        val fields: Array[String] = x.split(" ")
        ((fields(1), fields(4)), 1)
      }
    )
    val proAndAdCount: RDD[((String, String), Int)] = provinceAndAdOne.reduceByKey(_+_)

    val proAdAndCount: RDD[(String, (String, Int))] = proAndAdCount.map {
      x => {
        (x._1._1, (x._1._2, x._2))
      }
    }

    val proviceGroup: RDD[(String, Iterable[(String, Int)])] = proAdAndCount.groupByKey()

    /*
    //方案一：
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = proviceGroup.mapValues {
      x => {
        //先转换成集合，然后根据点击数逆序排序，取前3个
        //x.toList.sortWith((x, y) => (x._2 > y._2)).take(3)
        x.toList.sortBy((x)=>(x._2)).reverse.take(3)
      }
    }*/

    //方案二：
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = proviceGroup.map {
      x => {
        val temp: List[(String, Int)] = x._2.toList.sortWith((x, y) => (x._2 > y._2)).take(3)
        // x.toList.sortWith(_._2 > _._2).take(3)
        (x._1, temp)
      }
    }
    provinceAdTop3.collect().foreach(println)
    sc.stop()

  }

}
