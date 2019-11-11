package com.atguigu.sparksql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkSqlWithMysql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("spark://hadoop102:7077")
      .appName("SparkSqlWithMysql")
      .getOrCreate()

    import spark.implicits._

    //定义jdbc相关参数信息
    val conn = new Properties()

  }
}
