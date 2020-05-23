package com.bawei.gmall.publisher.service;

import java.util.Map;

public interface PublisherService {

    // 查询日活总数
    Long getDauTotal(String date);

    // 分时查询
    Map<String, Long> getDauHourCount(String date);

    // 查询交易额总数
    Double getOrderAmount(String date);

    // 查询交易额分时数据
    Map<String, Double> getOrderHourAmount(String date);

    // 查询ES结果
    Map<String, Object> getSaleDetailFromES(String date, String keyWord, int pageNo, int pageSize);
}
