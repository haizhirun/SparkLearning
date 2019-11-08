package com.atguigu.practice

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MysqlRDD")
    val sc = new SparkContext(conf)

    //1.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop102:3306/rdd"
    val userName = "root"
    val pwd = "123456"

    /*
    class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
     */
    //2.创建jdbcRDD
    val rdd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, pwd)
      },
      "select * from rddtable where id >= ? and id <= ?;",
      1,
      5,
      1,
      r => (r.getInt(1), r.getString(2))
    )
    println(rdd.count())
    rdd.foreach(println)

    sc.stop()

  }
}
