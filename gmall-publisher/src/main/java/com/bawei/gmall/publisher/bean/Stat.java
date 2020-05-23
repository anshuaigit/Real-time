package com.bawei.gmall.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @ClassName stat
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/13 11:29
 * @Version V1.0
 **/
@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<Option> options;
}
