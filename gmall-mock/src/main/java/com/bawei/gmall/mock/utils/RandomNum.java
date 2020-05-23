package com.bawei.gmall.mock.utils;

import java.util.Random;

/**
 * @ClassName RandomNum
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/6 9:02
 * @Version V1.0
 **/
public class RandomNum {
    public static final int getRandInt(int fromNum, int toNum) {
        return fromNum + new Random().nextInt(toNum - fromNum + 1);
    }
}

