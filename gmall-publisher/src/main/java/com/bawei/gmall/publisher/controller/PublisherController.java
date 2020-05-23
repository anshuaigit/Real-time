package com.bawei.gmall.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.bawei.gmall.publisher.bean.Option;
import com.bawei.gmall.publisher.bean.Stat;
import com.bawei.gmall.publisher.service.PublisherService;
import org.apache.commons.lang.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName PublisherController
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/10 9:42
 * @Version V1.0
 **/
@RestController
public class PublisherController {

    @Autowired
    private PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        Long dauTotal = publisherService.getDauTotal(date);
        List<Map> maps = new ArrayList<>();
        Map dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);
        Map newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", 233);
        Map orderMap = new HashMap();
        orderMap.put("id", "order_amount");
        orderMap.put("name", "新增交易额");
        orderMap.put("value", publisherService.getOrderAmount(date));
        maps.add(dauMap);
        maps.add(newMidMap);
        maps.add(orderMap);
        return JSON.toJSONString(maps);
    }

    @GetMapping("realtime-hour")
    public String getDauHourCount(@RequestParam("id") String id, @RequestParam("date") String date) {
        Map resultMap = new HashMap(2);
        if ("order_amount".equals(id)) {
            Map<String, Double> todayOrderHourAmount = publisherService.getOrderHourAmount(date);
            Map<String, Double> yesterdayOrderHourAmount = publisherService.getOrderHourAmount(getYesterDayString(date));
            resultMap.put("today", todayOrderHourAmount);
            resultMap.put("yesterday", yesterdayOrderHourAmount);
            return JSON.toJSONString(resultMap);
        } else if ("dau".equals(id)) {
            Map<String, Long> toDayCount = publisherService.getDauHourCount(date);
            Map<String, Long> yesterDayCount = publisherService.getDauHourCount(getYesterDayString(date));
            resultMap.put("today", toDayCount);
            resultMap.put("yesterday", yesterDayCount);
            return JSON.toJSONString(resultMap);
        }
        return null;
    }

    /**
     * 查询灵活分析结果
     *
     * @param date
     * @param keyword
     * @param startpage
     * @param size
     * @return
     */
    @GetMapping("sale_detail")
    public  String getSaleDetail(@RequestParam("date") String date,@RequestParam("keyword")String keyword,@RequestParam("startpage")int startpage,@RequestParam("size")int size ){
        //根据参数查询es

        Map<String, Object> saleDetailMap = publisherService.getSaleDetailFromES(date, keyword, startpage, size);
        Long total =(Long)saleDetailMap.get("total");
        List saleList =(List) saleDetailMap.get("saleList");
        Map genderMap =(Map) saleDetailMap.get("genderMap");
        Map ageMap = (Map)saleDetailMap.get("ageMap");


        Long maleCount =(Long)genderMap.get("M");
        Long femaleCount =(Long)genderMap.get("F");

        Double maleRatio= Math.round(maleCount*1000D/total)/10D;
        Double femaleRatio= Math.round(femaleCount*1000D/total)/10D;

        List genderOptionList=new ArrayList();
        genderOptionList.add( new Option("男",maleRatio));
        genderOptionList.add( new Option("女",femaleRatio));

        Stat genderStat = new Stat("性别占比", genderOptionList);


        Long age_20count=0L;
        Long age20_30count=0L;
        Long age30_count=0L;
        for (Object o : ageMap.entrySet()) {
            Map.Entry entry = (Map.Entry) o;
            String ageString = (String)entry.getKey();
            Long ageCount = (Long)entry.getValue();
            if(Integer.parseInt(ageString)<20){
                age_20count+=ageCount;
            }else if(Integer.parseInt(ageString)>=20 &&Integer.parseInt(ageString)<=30){
                age20_30count+=ageCount;
            }else{
                age30_count+=ageCount;
            }
        }

        Double age_20Ratio= Math.round(age_20count*1000D/total)/10D;
        Double age20_30Ratio= Math.round(age20_30count*1000D/total)/10D;
        Double age30_Ratio= Math.round(age30_count*1000D/total)/10D;

        List ageOptionList=new ArrayList();
        ageOptionList.add( new Option("20岁以下",age_20Ratio));
        ageOptionList.add( new Option("20岁到30岁",age20_30Ratio));
        ageOptionList.add( new Option("30岁以上",age30_Ratio));

        Stat ageStat = new Stat("年龄段占比", ageOptionList);

        List statList=new ArrayList();
        statList.add(genderStat);
        statList.add(ageStat);


        Map finalResultMap=new HashMap();
        finalResultMap.put("total",total);
        finalResultMap.put("stat",statList);
        finalResultMap.put("detail" ,saleList);


        return JSON.toJSONString(finalResultMap);
    }

    /**
     * 获取前一天的私有方法
     *
     * @param date
     * @return
     */
    private String getYesterDayString(String date) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String yesterDayString = null;
        try {
            Date today = dateFormat.parse(date);
            Date yesterDay = DateUtils.addDays(today, -1);
            yesterDayString = dateFormat.format(yesterDay);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return yesterDayString;
    }
}
