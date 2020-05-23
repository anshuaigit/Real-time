package com.bawei.gmall.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bawei.gmall.common.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName LoggerController
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/6 10:21
 * @Version V1.0
 **/
@Slf4j
@RestController
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("log")
    public String dolog(String logString) {
        // 1. 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts", System.currentTimeMillis());
        // 2. 落盘
        String jsonString = jsonObject.toJSONString();
        log.info(jsonString);
        // 3. 区分不同的日志类型
        if ("startup".equals(jsonObject.getString("type"))) {
            // 启动日志
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonString);
        } else {
            // 事件日志
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonString);
        }
        return "success";
    }
}
