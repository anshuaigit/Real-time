package com.bawei.gmall.canal.client;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @ClassName CanalClient
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/10 11:23
 * @Version V1.0
 **/
public class CanalClient {
    public static void main(String[] args) {
        // todo : 1. 建立连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example", "", "");

        while (true) {
            // 1. 建立连接
            canalConnector.connect();
            // 2. 订阅并监控数据
            canalConnector.subscribe("gmall.*");
            // 3. 获取数据
            Message message = canalConnector.get(100);
            // 4. 处理消息 entry -> RowChange -> RowDataList -> RowData
            if (message.getEntries().size() == 0) {
                System.out.println("现在没有数据,休息一下");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        // 5. 反序列化
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        // 6. 获取数据集合
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        // 7. 获取sql类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 8. 获取具体的表
                        String tableName = entry.getHeader().getTableName();
                        // 9. 数据同步处理逻辑
                        CanalHanlder canalHanlder = new CanalHanlder(tableName, eventType, rowDatasList);
                        // 10 .执行handle
                        canalHanlder.handle();
                    }
                }
            }
        }
    }
}
