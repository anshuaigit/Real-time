package com.bawei.gmall.mock.utils;

/**
 * @ClassName RanOpt
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/6 9:03
 * @Version V1.0
 **/
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}

