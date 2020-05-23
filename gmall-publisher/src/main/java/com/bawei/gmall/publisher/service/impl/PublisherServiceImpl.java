package com.bawei.gmall.publisher.service.impl;

import com.bawei.gmall.publisher.bean.OrderHourAmountBean;
import com.bawei.gmall.publisher.dao.DauMapper;
import com.bawei.gmall.publisher.dao.OrderMapper;
import com.bawei.gmall.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName PublisherServiceImpl
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/10 9:35
 * @Version V1.0
 **/
@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    private DauMapper dauMapper;
    @Autowired
    private OrderMapper orderMapper;
    @Autowired
    private JestClient jestClient;


    /**
     * 查询日活总数
     *
     * @param date
     * @return
     */
    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    /**
     * 获取分时数据
     *
     * @param date
     * @return
     */
    @Override
    public Map<String, Long> getDauHourCount(String date) {
        List<Map> dauHourCount = dauMapper.getDauHourCount(date);
        Map<String, Long> hashMap = new HashMap(dauHourCount.size());
        dauHourCount.forEach(data -> hashMap.put((String) data.get("LH"), (Long) data.get("CT")));
        return hashMap;
    }

    /**
     * 查询交易额总数
     *
     * @param date
     * @return
     */
    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    /**
     * 查询交易额分时数据
     *
     * @param date
     * @return
     */
    @Override
    public Map<String, Double> getOrderHourAmount(String date) {
        Map<String, Double> hourMap = new HashMap<>();
        List<OrderHourAmountBean> orderHourAmount = orderMapper.getOrderHourAmount(date);
        orderHourAmount.forEach(data -> hourMap.put(data.getCreateHour(), data.getSumOrderAmount()));
        return hourMap;
    }

    /**
     * 查询ES数据
     *
     * @param date
     * @param keyWord
     * @param pageNo
     * @param pageSize
     * @return
     */
    @Override
    public Map<String, Object> getSaleDetailFromES(String date, String keyWord, int pageNo, int pageSize) {
//        String s = "GET gmall_sale_detail/_search\n" +
//                "{\n" +
//                "  \"query\" : {\n" +
//                "    \"bool\": {\n" +
//                "      \"filter\": {\n" +
//                "        \"term\": {\n" +
//                "          \"dt\": \"2020-03-13\"\n" +
//                "        }\n" +
//                "      },\n" +
//                "      \"must\": {\n" +
//                "        \"match\" : {\n" +
//                "          \"sku_name\" : {\n" +
//                "            \"query\" : \"小米路由器\",\n" +
//                "            \"operator\" : \"or\"\n" +
//                "          }\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  },\n" +
//                "  \"aggs\":{\n" +
//                "        \"groupby_gender\" : {\n" +
//                "          \"terms\": {\n" +
//                "            \"field\": \"user_gender\",\n" +
//                "            \"size\": 2\n" +
//                "          }\n" +
//                "        }\n" +
//                "      },\n" +
//                "  \"from\" : 1,\n" +
//                "  \"size\" : 3\n" +
//                "}";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 构造查询条件和过滤条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt", date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name", keyWord).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        // 构建聚合条件
        TermsBuilder genderAggs = AggregationBuilders.terms("group_gender").field("user_gender").size(2);
        TermsBuilder ageAggs = AggregationBuilders.terms("group_age").field("user_age").size(100);
        searchSourceBuilder.aggregation(genderAggs);
        searchSourceBuilder.aggregation(ageAggs);

        // 构建分页条件
        searchSourceBuilder.from((pageNo - 1) * pageSize);
        searchSourceBuilder.size(pageSize);
        // 打印查询条件
        System.out.println(searchSourceBuilder.toString());
        // 执行查询
        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        // 创建容器
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // 调用查询接口
            SearchResult se = jestClient.execute(search);
            // 总数
            resultMap.put("total", se.getTotal());
            // 结果集
            List<SearchResult.Hit<Map, Void>> hits = se.getHits(Map.class);
            List<Map> saleList = new ArrayList<>(hits.size());
            // 遍历结果集
            for (SearchResult.Hit<Map, Void> hit : hits) {
                saleList.add(hit.source);
            }
            // 返回结果集
            resultMap.put("saleList", saleList);
            // 性别聚合
            Map genderMap = new HashMap();
            List<TermsAggregation.Entry> genderBuckets = se.getAggregations().getTermsAggregation("group_gender").getBuckets();
            // 遍历性别集合
            for (TermsAggregation.Entry genderBucket : genderBuckets) {
                genderMap.put(genderBucket.getKey(), genderBucket.getCount());
            }
            // 返回性别集合
            resultMap.put("genderMap", genderMap);
            // 年龄聚合
            Map ageMap = new HashMap();
            List<TermsAggregation.Entry> ageBuckets = se.getAggregations().getTermsAggregation("group_age").getBuckets();
            // 遍历性别集合
            for (TermsAggregation.Entry ageBucket : ageBuckets) {
                ageMap.put(ageBucket.getKey(), ageBucket.getCount());
            }
            // 返回性别集合
            resultMap.put("ageMap", ageMap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultMap;
    }
}
