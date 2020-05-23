package com.bawei.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.bawei.gmall.common.GmallConstants
import com.bawei.gmall.realtime.bean.OrderInfo
import com.bawei.gmall.realtime.utils.KafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val sparkContext = new StreamingContext(conf, Seconds(5))
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, sparkContext)

    // todo : 1. 补充时间戳以及敏感字段的脱敏
    val result: DStream[OrderInfo] = inputDStream.map {
      record => {
        val jsonStr = record.value()
        // json -> 样例类
        val orderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
        val dateTimeArray = orderInfo.create_time.split(" ")
        orderInfo.create_date = dateTimeArray(0)
        val hourStr = dateTimeArray(1).split(":")(0)
        orderInfo.create_hour = hourStr
        // 手机号脱敏
        val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
        orderInfo.consignee_tel = tuple._1 + "*******"
        orderInfo
      }
    }

    // todo : 2. 将数据存入HBase
    // 一定要先建表
    result.foreachRDD {
      rdd => {
        rdd.saveToPhoenix("gmall_order_info", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    }

    sparkContext.start()
    sparkContext.awaitTermination()
  }
}
