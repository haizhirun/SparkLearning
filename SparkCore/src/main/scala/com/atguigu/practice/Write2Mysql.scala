package com.atguigu.practice

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

object Write2Mysql {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Write2Mysql")
    val sc = new SparkContext(sparkConf)

    val data = sc.parallelize(List("guikaisheng","zhaochang","mike","aaa","bbb","ccc"))

    data.foreachPartition(insertData)
    println("插入成功")
  }

  def insertData(iterator: Iterator[String]): Unit ={
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    val conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/rdd","root","123456")

    iterator.foreach(
      data => {
        println(data)
        val ps = conn.prepareStatement("insert into rddtable(name) values(?)")
        ps.setString(1,data)
        ps.executeUpdate()
      }
    )

  }
}
