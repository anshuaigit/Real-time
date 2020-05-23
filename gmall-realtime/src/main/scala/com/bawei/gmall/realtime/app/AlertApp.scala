package com.bawei.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.bawei.gmall.common.GmallConstants
import com.bawei.gmall.realtime.bean.{AlertInfo, EventInfo}
import com.bawei.gmall.realtime.utils.{EsUtil, KafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util

import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._

object AlertApp {
  //  拆 :
  //    数据来源 : event日志 {格式}
  //  同一设备 : mid
  //  5分钟内  : 窗口大小 5分钟  窗口大小:数据范围 滑动步长 : 统计频率
  //  用不同账号登录并领取优惠劵且没有浏览商品 : groupbykey filter
  //    产生一条预警日志{格式} : map(转换格式)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("alert_app").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    // 1. 转换结构便于后续操作
    val eventInfoDStream = inputDStream.map {
      record => {
        val eventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
        eventInfo
      }
    }

    // 2. 5分钟内  : 窗口大小 5分钟  窗口大小:数据范围 滑动步长 : 统计频率
    val windowDStream: DStream[EventInfo] = eventInfoDStream.window(Seconds(300), Seconds(5))
    // 3. 同一设备 mid groupByKey
    val groupByMidDStream: DStream[(String, Iterable[EventInfo])] = windowDStream.map(eventInfo => (eventInfo.mid, eventInfo)).groupByKey()
    // 4. 用不同账号登录并领取优惠劵且没有浏览商品 : groupbykey filter
    val checkDStream: DStream[(Boolean, AlertInfo)] = groupByMidDStream.map {
      case (mid, eventInfoItr) => {
        val couponUidSet = new util.HashSet[String]()
        val itemsSet = new util.HashSet[String]()
        val eventList = new util.ArrayList[String]()
        // 做一个浏览商品的标记
        var isClickItem = false
        // 判断用户是否有浏览商品的行为
        breakable {
          for (elem <- eventInfoItr) {
            eventList.add(elem.evid)
            if (elem.evid == "coupon") {
              // 点击了购物券需要拿到登录的用户
              couponUidSet.add(elem.uid)
              itemsSet.add(elem.itemid)
            }
            if (elem.evid == "clickItem") {
              isClickItem = true
              break()
            }
          }
        }
        // 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，
        // 并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。同一设备，每分钟只记录一次预警。
        (couponUidSet.size() >= 3 && !isClickItem, AlertInfo(mid, couponUidSet, itemsSet, eventList, System.currentTimeMillis()))
      }
    }
    //    checkDStream.foreachRDD{
    //      rdd => println(rdd.collect().mkString("\n"))
    //    }
    // 5. 多变少 groupbykey filter
    val alterDStream: DStream[AlertInfo] = checkDStream.filter(_._1).map(_._2)

    // 6. 保存到ES中
    alterDStream.foreachRDD {
      rdd: RDD[AlertInfo] =>
        rdd.foreachPartition {
          alertItr => {
            val list: List[AlertInfo] = alertItr.toList
            // 提取主键mid + 分钟 组合为主键,在利用组合主键去重
            val tuples = list.map(alertInfo => (alertInfo.mid + "_" + alertInfo.ts / 1000 / 60, alertInfo))
            // 批量保存
            EsUtil.indexBulk(GmallConstants.ES_INDEX_ALERT,tuples)
          }
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
