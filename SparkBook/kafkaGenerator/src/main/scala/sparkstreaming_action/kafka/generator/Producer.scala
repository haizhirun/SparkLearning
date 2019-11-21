package sparkstreaming_action.kafka.generator

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.{BufferedSource, Source}
import scala.util.Random

/**
  * 模拟一些数据发送到kafka中
  */
object Producer {
  def main(args: Array[String]): Unit = {

    val topic  = args(0)

    val brokers = args(1)

    //设置一个随机数
    val random = new Random()

    //设置kafka配置项
    val props = new Properties()

    props.put("bootstrap.servers",brokers)
    StringSerializer
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](props)

    val t = System.currentTimeMillis()

    //模拟用户名地址信息
    val nameAddrs: Map[String, String] = Map(
      "bob" -> "shanghai#200000",
      "amy" -> "beijing#100000",
      "alice" -> "shanghai#200000",
      "tom" -> "beijing#100000",
      "lulu" -> "hangzhou#310000",
      "nick" -> "shanghai#200000"
    )

    //模拟用户名电话数据
    val namePhones: Map[String, String] = Map(
      "bob" -> "15927085180",
      "amy" -> "13100678561",
      "alice" -> "13789980347",
      "tom" -> "13037198105",
      "lulu" -> "13889980453",
      "nick" -> "15879653420"
    )

    //生成模拟数据(name,addr,type:0)
    for (nameAddr <- nameAddrs) {
      val data = new ProducerRecord[String,String](topic,nameAddr._1,s"${nameAddr._1}\t${nameAddr._2}\t0")
      producer.send(data)
      //模拟发送时间，
      if(random.nextInt(100)<50){
        Thread.sleep(random.nextInt(10))
      }
    }
    //生成模拟数据(name,phont,type:1)
    for (namePhone <- namePhones) {
      val data = new ProducerRecord[String,String](topic,namePhone._1,s"${namePhone._1}\t${namePhone._2}\t1")
      producer.send(data)
      //模拟发送时间，
      if(random.nextInt(100)<50){
        Thread.sleep(random.nextInt(10))
      }
    }
    //每秒发送到kafka的数据条数
    System.out.println("send per second: " + (nameAddrs.size + namePhones.size)*1000/(System.currentTimeMillis()-t))
    producer.close()
  }
}
