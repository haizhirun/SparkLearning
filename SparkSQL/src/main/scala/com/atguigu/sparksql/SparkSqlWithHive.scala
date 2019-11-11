package com.atguigu.sparksql

import org.apache.spark.sql.SparkSession

object SparkSqlWithHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .enableHiveSupport() //代码中支持hive操作
      .master("local[*]")
      .appName("SparkSqlWithHive")
      .getOrCreate()


  }
}
