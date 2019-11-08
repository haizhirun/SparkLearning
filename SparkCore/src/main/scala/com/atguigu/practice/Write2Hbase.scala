package com.atguigu.practice

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 往hbase中写入数据
  */
object Write2Hbase {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Write2Hbase")
    val sc = new SparkContext(sparkConf)

    //1.创建HbaseConf
    val conf: Configuration = HBaseConfiguration.create()
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"stu")

    val initialRDD: RDD[(Int, String)] = sc.parallelize(List((1001,"aaa"),(1002,"bbb")))

    val localData = initialRDD.map(convert)
    localData.saveAsHadoopDataset(jobConf)
  }

  /**
    * 定义往hbase插入数据的方法
    */
  def convert(triple:(Int,String))={
    val put = new Put(Bytes.toBytes(triple._1))
    put.addImmutable(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(triple._2))
    (new ImmutableBytesWritable(),put)
  }
}
