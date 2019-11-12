package com.atguigu.datastructure

/**
  *
  * @param ordernumber 订单号
  * @param locationid 交易位置
  * @param dateid 交易日期
  */
case class tbStock(ordernumber:String,
                   locationid:String,
                   dateid:String
                  ) extends Serializable