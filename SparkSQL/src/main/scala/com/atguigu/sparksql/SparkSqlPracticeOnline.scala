package com.atguigu.sparksql

import com.atguigu.datastructure.tbStock
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}


object SparkSqlPracticeOnline {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("spark://hadoop102:7077")
      .config("spark.jars", "D:\\workspace\\ideaWorkSpace\\hust\\bigdata\\Spark-Learning\\SparkSQL\\src\\main\\resources\\spark-core_2.11-2.1.1.jar")
      .appName("SparkSqlPracticeOnline")
      .getOrCreate()

    import spark.implicits._

    //1.加载数据，使用sparkContext将数据加载为rdd,然后转换为DateSet,然后再创建一个临时视图保存
    val tbStockRdd: RDD[String] = spark.sparkContext.textFile("hdfs://hadoop102:9000/spark/data/input/tbStock.txt")

    //tbStockRdd.collect().foreach(println(_))
    val tbStockDS: Dataset[tbStock] = tbStockRdd.map(_.split(",")).map(line => {
      tbStock(line(0).trim, line(1).trim, line(2).trim)
    }).toDS()

    //报错：仍未解决：cannot assign instance of scala.collection.immutable.List$SerializationProxy to field org.apache.spark.rdd.RDD.org$apache$spark$rdd$RDD$$dependencies_ of type scala.collection.Seq in instance of org.apache.spark.rdd.MapPartitionsRDD
    tbStockDS.show()

    /*val tbStockDetailRdd: RDD[String] = spark.sparkContext.textFile("hdfs://hadoop102:9000/spark/data/input/tbStockDetail.txt")
    tbStockDetailRdd.map(_.split(",")).map(line => {
      //tbStockDetail(ordernumber:String,rownum:Int,itemid:String,number:Int,price:Double,amount:Double)
      tbStockDetail(line(0).trim,line(1).trim.toInt,line(2).trim,line(3))
    })*/
    spark.stop()
  }
}
