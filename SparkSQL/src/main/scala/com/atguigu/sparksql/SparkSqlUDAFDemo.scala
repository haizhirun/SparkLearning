package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlUDAFDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSqlUDAFDemo")
      .getOrCreate()

    import spark.implicits._

    //注册自定义函数
    spark.udf.register("myAverage",MyAverage)

    val df: DataFrame = spark.read.json("D:\\workspace\\ideaWorkSpace\\hust\\bigdata\\Spark-Learning\\SparkSQL\\src\\data\\employees.json")

    df.createOrReplaceTempView("employees")

    df.show()

    spark.sql("select myAverage(salary) as average_salary from employees").show()

    spark.stop()
  }



}
