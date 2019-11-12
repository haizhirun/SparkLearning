package com.atguigu.sparksql

import com.atguigu.datastructure.{tbDate, tbStock, tbStockDetail}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


object SparkSqlPracticeOffline {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkSqlPracticeOnline")
      .getOrCreate()

    import spark.implicits._

    val prefixPath = "D:\\workspace\\ideaWorkSpace\\hust\\bigdata\\Spark-Learning\\SparkSQL\\src\\data\\"

    //1.加载数据，使用sparkContext将数据加载为rdd,然后转换为DateSet,然后再创建一个临时视图保存
    val tbStockRdd: RDD[String] = spark.sparkContext.textFile(prefixPath + "tbStock.txt")

    //tbStockRdd.collect().foreach(println(_))
    val tbStockDS: Dataset[tbStock] = tbStockRdd.map(_.split(",")).map(line => {
      tbStock(line(0).trim, line(1).trim, line(2).trim)
    }).toDS()

    //tbStockDS.show()

    val tbStockDetailRdd: RDD[String] = spark.sparkContext.textFile(prefixPath + "tbStockDetail.txt")

    val tbStockDetailDS: Dataset[tbStockDetail] = tbStockDetailRdd.map(_.split(",")).map(line => {
      //tbStockDetail(ordernumber:String,rownum:Int,itemid:String,number:Int,price:Double,amount:Double)
      tbStockDetail(line(0).trim, line(1).trim.toInt, line(2).trim, line(3).trim.toInt, line(4).trim.toDouble, line(5).trim.toDouble)
    }).toDS()

    //tbStockDetailDS.show()

    val tbDateRdd: RDD[String] = spark.sparkContext.textFile(prefixPath + "tbDate.txt")

    val tbDateDS: Dataset[tbDate] = tbDateRdd.map(_.split(",")).map(line => {
      tbDate(line(0).trim,
        line(1).trim.toInt,
        line(2).trim.toInt,
        line(3).trim.toInt,
        line(4).trim.toInt,
        line(5).trim.toInt,
        line(6).trim.toInt,
        line(7).trim.toInt,
        line(8).trim.toInt,
        line(9).trim.toInt
      )
    }).toDS()

    //tbDateDS.show()

    //2.将上述三个DataSet注册为临时视图
    tbStockDS.createOrReplaceTempView("tbStock")
    tbStockDetailDS.createOrReplaceTempView("tbStockDetail")
    tbDateDS.createOrReplaceTempView("tbDate")

    //3.实现需求

    //3.1 : 计算所有订单中每年的销售单数、销售总额,数据有点问题，用来锻炼写sql语句
    val sqlOne : String = "select c.theyear,count(distinct(a.ordernumber)) orderSum, sum(b.amount) amountSum " +
                        "from tbStock a " +
                            "join tbStockDetail b on a.ordernumber = b.ordernumber " +
                            "join tbDate c on a.dateid = c.dateid " +
                            "group by c.theyear " +
                            "order by c.theyear"
//    println(sqlOne)
//    spark.sql(sqlOne).show()

    //3.2:计算所有订单每年最大金额订单的销售额

    /**
    //1.统计每年，每个订单一共有多少销售额
        select a.dateid,a.ordernumber,sum(b.amount) SumOfAmount
        from tbStock a
          join tbStockDetail b on a.ordernumber = b.ordernumber
        group by a.dateid,a.ordernumber;

        //2.以上一步为基础表，和tbDate中dateid join,求出每年最大金额订单的销售额

        select d.theyear,max(c.SumOfAmount)
          from
          (
            select a.dateid,a.ordernumber,sum(b.amount) SumOfAmount
              from tbStock a
                join tbStockDetail b on a.ordernumber = b.ordernumber
              group by a.dateid,a.ordernumber
          )c
          join tbDate d on c.dateid = d.dateid
        group by d.theyear
        order by d.theyear desc;
      */
   val sqlTwo : String = "select d.theyear,max(c.SumOfAmount)\n          from \n          (\n            select a.dateid,a.ordernumber,sum(b.amount) SumOfAmount\n              from tbStock a \n                join tbStockDetail b on a.ordernumber = b.ordernumber\n              group by a.dateid,a.ordernumber\n          )c \n          join tbDate d on c.dateid = d.dateid\n        group by d.theyear\n        order by d.theyear desc"

    //spark.sql(sqlTwo).show()

    //3.3 计算所有订单中每年最畅销货品
    //目标：统计每年最畅销货品（哪个货品销售额amount在当年最高，哪个就是最畅销货品）

    //val sqlThree : String = "select d.theyear,max(c.SumOfAmount)\n\tfrom \n\t(\n\tselect a.dateid,a.ordernumber,sum(b.amount) SumOfAmount\n\tfrom tbStock a \n\tjoin tbStockDetail b on a.ordernumber = b.ordernumber\ngroup by a.dateid,a.ordernumber\n\t) c\n\tjoin tbDate d\non c.dateid = d.dateid\ngroup by d.theyear\norder by d.theyear desc"
    val sqlThree = "SELECT DISTINCT e.theyear, e.itemid, f.MaxOfAmount\nFROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS SumOfAmount\nFROM tbStock a\nJOIN tbStockDetail b ON a.ordernumber = b.ordernumber\nJOIN tbDate c ON a.dateid = c.dateid\nGROUP BY c.theyear, b.itemid\n) e\nJOIN (SELECT d.theyear, MAX(d.SumOfAmount) AS MaxOfAmount\nFROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS\nSumOfAmount\nFROM tbStock a\nJOIN tbStockDetail b ON a.ordernumber = b.ordernumber\nJOIN tbDate c ON a.dateid = c.dateid\nGROUP BY c.theyear, b.itemid\n) d\nGROUP BY d.theyear\n) f ON e.theyear = f.theyear\nAND e.SumOfAmount = f.MaxOfAmount\nORDER BY e.theyear"
//    val sqlThree = "select e.theyear,e.itemid,f.MaxOfAmount\n\tfrom\n(\nselect c.theyear,b.itemid,sum(b.amount) SumOfAmount\nfrom tbStock a\n\tjoin tbStockDetail b on a.ordernumber = b.ordernumber\n\tjoin tbDate c on a.dateid = c.dateid\ngroup by c.theyear,b.itemid\n) e join\n(\nselect d.theyear,max(d.SumOfAmount) MaxOfAmount\nfrom (\nselect c.theyear,b.itemid,sum(b.amount) SumOfAmount\nfrom tbStock a\n\tjoin tbStockDetail b on a.ordernumber = b.ordernumber\n\tjoin tbDate c on a.dateid = c.dateid\ngroup by c.theyear,b.itemid;\n)d \ngroup by d.theyear\n)f\non e.theyear = f.theyear and e.SumOfAmount = f.MaxOfAmount\norder by e.theyear"
    spark.sql(sqlThree).show()
    //    spark.sql("select * from tbDate").show()
    spark.stop()
  }
}
