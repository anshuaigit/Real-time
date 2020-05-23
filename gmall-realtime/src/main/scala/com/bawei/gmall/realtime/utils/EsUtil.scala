package com.bawei.gmall.realtime.utils

import java.util
import java.util.Objects

import com.bawei.gmall.realtime.bean.AlertInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object EsUtil {

  // ES的地址
  private val ES_HOST = "http://hadoop102"
  // ES的端口号
  private val ES_PORT = 9200
  // ES的工厂类
  private var factory: JestClientFactory = null

  /**
    * 获取客户端连接器
    *
    * @return
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端连接器
    *
    * @param client
    */
  def close(client: JestClient): Unit = {
    // 只有客户端非空才能关闭,为空关闭客户端会报异常
    if (!Objects.isNull(client))
      try
        client.shutdownClient()
      catch {
        case e: Exception => e.printStackTrace()
      }
  }

  /**
    * 建立连接
    */
  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_PORT).multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000)
      .readTimeout(10000).build())
  }

  /**
    * 批量发送数据
    *
    * @param indexName
    * @param tuples
    */
  def indexBulk(indexName: String, tuples: List[(String, Any)]): Unit = {
    if (tuples.size > 0) {
      // 1. 获取连接
      val client = getClient
      // 2. 获取批发送器
      val bulkBuilder = new Bulk.Builder
      // 3. 遍历tuple数据
      for ((id, data) <- tuples) {
        // 创建ES表处理器 ES数据结构 : indes = table和database docment = row
        val index = new Index.Builder(data).index(indexName).`type`("_doc").id(id).build()
        // 发送数据
        bulkBuilder.addAction(index)
      }
      // 构建bulk
      val bulk = bulkBuilder.build()
      // 打印插入失败的日志
      val items: util.List[BulkResult#BulkResultItem] = client.execute(bulk).getItems
      // 打印日志
      println("成功保存 " + items.size() + " 条数据")
      // 关闭连接
      close(client)
    }
  }
}
