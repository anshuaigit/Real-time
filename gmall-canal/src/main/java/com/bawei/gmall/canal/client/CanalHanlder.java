package com.bawei.gmall.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.bawei.gmall.canal.client.utils.KafkaSender;
import com.bawei.gmall.common.GmallConstants;

import java.util.List;

/**
 * @ClassName CanalHanlder
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/10 11:40
 * @Version V1.0
 **/
public class CanalHanlder {
    // 表名
    private String tableName;
    // Sql类型
    private CanalEntry.EventType eventType;
    // 数据集合
    private List<CanalEntry.RowData> rowDatasList;

    public CanalHanlder(String tableName, CanalEntry.EventType entryType, List<CanalEntry.RowData> rowDatasList) {
        this.tableName = tableName;
        this.eventType = entryType;
        this.rowDatasList = rowDatasList;
    }

    public void handle() {
        if (tableName.equals("order_info") && eventType.equals(CanalEntry.EventType.INSERT)) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_ORDER);
            }
        }
        //  追加用户表user_info
        else if (tableName.equals("user_info") && (eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE))) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_USER_INFO);
            }
        }
        // 追加订单详情表order_detail
        else if (tableName.equals("order_detail") && (eventType.equals(CanalEntry.EventType.INSERT))) {
            for (CanalEntry.RowData rowData : rowDatasList) {
                sendKafka(rowData, GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
            }
        }
    }

    /**
     * 发送Kafka
     *
     * @param rowData
     * @param topic
     */
    private void sendKafka(CanalEntry.RowData rowData, String topic) {
        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
        JSONObject jsonObject = new JSONObject();
        for (CanalEntry.Column column : afterColumnsList) {
            System.out.println(column.getName() + "------>" + column.getValue());
            // 发送数据到对应的topic中
            jsonObject.put(column.getName(), column.getValue());
        }
        String rowJson = jsonObject.toJSONString();
        KafkaSender.send(topic, rowJson);
    }
}
