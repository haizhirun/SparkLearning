package com.atguigu.trans

import org.apache.spark.rdd.RDD

class Search(query:String)  {

  def isMatch(s:String)={
    s.contains(query)
  }

  def getMatch1(rdd:RDD[String])={
    rdd.filter(isMatch)
  }

  def getMatch2(rdd:RDD[String])={
    val str = this.query
    rdd.filter(x=>x.contains(str))
  }

}
