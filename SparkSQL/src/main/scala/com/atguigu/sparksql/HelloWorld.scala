package com.atguigu.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object HelloWorld {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("HelloWorld").getOrCreate()

    //导入隐式转换，spark为前面创建那个对象，如果前面名称是spark2,则导入的是spark2
    import spark.implicits._

    val df: DataFrame = spark.read.json("D:\\workspace\\ideaWorkSpace\\hust\\bigdata\\Spark-Learning\\SparkSQL\\src\\data\\people.json")

    df.show()

    df.filter($"age">21).show()

    df.select("name").show()

    df.createOrReplaceTempView("people")

    spark.sql("select * from people where age > 21").show()

    //关闭连接
    spark.stop()

  }

}
