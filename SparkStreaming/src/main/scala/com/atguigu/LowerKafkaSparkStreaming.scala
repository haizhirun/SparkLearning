package com.atguigu


import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext, kafka}
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err

import scala.collection.mutable

object LowerKafkaSparkStreaming {


  def main(args: Array[String]): Unit = {
    //创建配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LowerKafkaSparkStreaming")

    val ssc = new StreamingContext(sparkConf,Seconds(3))


    val brokers:String = "hadoop102:9092,hadoop103:9092,hadoop104:9092"
    val topic : String = "sparkStreaming2kafka1"
    val group : String = "spark2kafka"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //封装kafka
    val kafkaPara: Map[String, String] = Map[String, String](
      "zookeeper.connect"->"hadoop102:2181,hadoop103:2181,hadoop104:2181/kafka",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )

    val kafkaCluster = new KafkaCluster(kafkaPara)

    //获取当前消费的offset
    val fromOffset:Map[TopicAndPartition,Long] = getOffSet(kafkaCluster,group,topic)

    //消费kafka数据创建DStream
    val kafkaDstream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaPara,
      fromOffset,
      (message:MessageAndMetadata[String,String]) => message.message()
    )

    kafkaDstream.print()

    setOffset(kafkaCluster,group,kafkaDstream)

    ssc.start()
    ssc.awaitTermination()
  }

  //获取offset
  def getOffSet(kafkaCluster: KafkaCluster, group: String, topic: String): Map[TopicAndPartition, Long] = {
    //定义一个最终返回值：主题分区->offset
    val partitionToLong = new mutable.HashMap[TopicAndPartition,Long]()
    //根据指定的

    val topicAndPartitionsEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //判断分区是否存在
    if(topicAndPartitionsEither.isRight){

      //分区信息不为空，取出分区信息
      val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionsEither.right.get

      //获取消费者消费数据的进度
      val topicAndPartitionToLongEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group,topicAndPartitions)

      //判断offset是否存在
      if (topicAndPartitionToLongEither.isLeft) {
        //offset不存在（该消费者组未消费过），遍历每一个分区
        for(topicAndPartition <- topicAndPartitions){
          //使用simpleConsumer获取该分区的最小offset

          partitionToLong += (topicAndPartition->0L)
        }
      }else{
        //取出offset
        val value: Map[TopicAndPartition, Long] = topicAndPartitionToLongEither.right.get

        partitionToLong ++= value
      }

    }

    partitionToLong.toMap
  }

  /*
    原因总结:java.lang.IllegalArgumentException: requirement failed: numRecords must not be negative
    当删除一个topic时，zk中的offset信息并没有被清除，因此KafkaDirectStreaming再次启动时仍会得到旧的topic offset为old_offset，作为fromOffset。
    当新建了topic后，使用untiloffset计算逻辑，得到untilOffset为0（如果topic已有数据则>0）；
    再次被启动的KafkaDirectStreaming Job通过异常的计算逻辑得到的rdd numRecords值为可计算为：
    numRecords = untilOffset - fromOffset(old_offset)
    当untilOffset < old_offset时，此异常会出现，对于新建的topic这种情况的可能性很大

    解决方法
    思路
    根据以上分析，可在确定KafkaDirectStreaming 的fromOffsets时判断fromOffset与untiloffset的大小关系，当untilOffset < fromOffset时，矫正fromOffset为offset初始值0。

    流程
    从zk获取topic/partition 的fromOffset（获取方法链接）
    利用SimpleConsumer获取每个partiton的lastOffset（untilOffset ）
    判断每个partition lastOffset与fromOffset的关系
    当lastOffset < fromOffset时，将fromOffset赋值为0
    通过以上步骤完成fromOffset的值矫正。
   */
  //保存offseet
  def setOffset(kafkaCluster: KafkaCluster, group:String,kafkaDstream: InputDStream[String]) = {

    kafkaDstream.foreachRDD(rdd=>{

      val partitionToLong = new mutable.HashMap[TopicAndPartition,Long]()

      //取出rdd中的offset,记住
      val offsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

      //获取所有分区的offsetRange
      val ranges: Array[OffsetRange] = offsetRanges.offsetRanges

      for (range <- ranges) {
        partitionToLong += (range.topicAndPartition()->range.untilOffset)
      }
      kafkaCluster.setConsumerOffsets(group,partitionToLong.toMap)
    })

  }

}
