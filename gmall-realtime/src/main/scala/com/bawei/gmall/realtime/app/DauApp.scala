package com.bawei.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.bawei.gmall.common.GmallConstants
import com.bawei.gmall.realtime.bean.StartUpLog
import com.bawei.gmall.realtime.utils.KafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)
    //    inputDStream.foreachRDD(rdd => println(rdd.map(_.value()).collect().mkString("\n")))
    // todo : 1. 统计日活
    // todo : 2. 转换样例类,补充日期
    val startUpLogDStream = inputDStream.map {
      record => {
        val str = record.value()
        val startUpLog = JSON.parseObject(str, classOf[StartUpLog])
        val dateTimeString = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(startUpLog.ts))
        val dates: Array[String] = dateTimeString.split(" ")
        startUpLog.logDate = dates(0)
        startUpLog.logHour = dates(1)
        startUpLog
      }
    }

    // todo : 3. 利用用户清单进行过滤和去重,只保留清单中不存在的用户访问记录;
    val filterDStream: DStream[StartUpLog] = startUpLogDStream.transform {
      rdd => {
        println("过滤前 : " + rdd.count())
        val jedis = new Jedis("hadoop102", 6379)
        val dauKey = "dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
        val dauSet = jedis.smembers(dauKey)
        val dauBroadcase = ssc.sparkContext.broadcast(dauSet)
        val filterRdd = rdd.filter {
          startUpLog => !dauBroadcase.value.contains(startUpLog.mid)
        }
        println("过滤后 : " + filterRdd.count())
        filterRdd
      }
    }
    //    filterDStream.foreachRDD(rdd => println(rdd.collect().mkString("\n")))

    // todo : 4. 批次内进行去重,按照mid进行分组,每组取第一个值
    val groupByKeyMidDStream: DStream[(String, Iterable[StartUpLog])] = filterDStream.map(startUpLog => (startUpLog.mid, startUpLog)).groupByKey()
    val realFilterDStream = groupByKeyMidDStream.flatMap {
      case (mid, startUpLogItr) => {
        startUpLogItr.take(1)
      }
    }
    // 缓存
    realFilterDStream.cache()

    // todo :5. 去重mid
    realFilterDStream.foreachRDD { rdd =>
      rdd.foreachPartition { startUpLogItr =>
        val jedis = new Jedis("hadoop102", 6379)
        for (elem <- startUpLogItr) {
          val key = "dau:" + elem.logDate
          jedis.sadd(key, elem.mid)
          println("mid : : : " + elem.mid)
        }
        jedis.close()
      }
    }

    // todo : 6. 将去重后的结果写入HBase
    realFilterDStream.foreachRDD {
      rdd => {
        rdd.saveToPhoenix("GMALL_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          new Configuration,
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    }

    println("启动流程")
    ssc.start()
    ssc.awaitTermination()
  }
}
