package com.atguigu.datastructure

/**
  *
  * @param ordernumber 订单号
  * @param rownum 行号
  * @param itemid 货品
  * @param number 数量
  * @param price 价格
  * @param amount 销售额
  */
case class tbStockDetail(ordernumber:String,
                         rownum:Int,
                         itemid:String,
                         number:Int,
                         price:Double,
                         amount:Double
                        ) extends Serializable