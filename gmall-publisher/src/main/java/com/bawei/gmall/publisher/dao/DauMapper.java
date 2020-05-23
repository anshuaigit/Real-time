package com.bawei.gmall.publisher.dao;

import java.util.List;
import java.util.Map;

/**
 * @ClassName DauMapper
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/10 9:27
 * @Version V1.0
 **/
public interface DauMapper {

    // 查询日活总数
    Long getDauTotal(String date);

    // 分时查询
    List<Map> getDauHourCount(String date);
}
