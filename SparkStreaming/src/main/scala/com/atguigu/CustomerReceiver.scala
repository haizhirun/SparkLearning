package com.atguigu

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class CustomerReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //启动时调用该方法，用来给读数据并将数据发送给spark
  override def onStart(): Unit = {
    new Thread("Receiver"){
      override def run(): Unit = {
        receive()
      }
    }.start()
  }

  def receive(): Unit = {
    val socket = new Socket(host,port)

    //接受从端口读取过来的数据
    var input : String = null

    //创建一个BufferedReader用于读取端口传来的数据
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,StandardCharsets.UTF_8))

    input = reader.readLine()

    while(!isStopped() && input != null){
      /**
        * Store a single item of received data to Spark's memory.
        * These single items will be aggregated together into data blocks before
        * being pushed into Spark's memory.
        */
      store(input)
      input = reader.readLine()
    }

    //跳出循环则关闭任务
    reader.close()
    socket.close()

    //重启任务
    restart("restart")

  }
  override def onStop(): Unit = ???
}
