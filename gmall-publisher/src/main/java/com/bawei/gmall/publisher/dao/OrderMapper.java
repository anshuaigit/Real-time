package com.bawei.gmall.publisher.dao;

import com.bawei.gmall.publisher.bean.OrderHourAmountBean;

import java.util.List;

public interface OrderMapper {

    // 查询总数
    Double getOrderAmount(String date);

    // 查询分时数据
    List<OrderHourAmountBean> getOrderHourAmount(String date);
}
