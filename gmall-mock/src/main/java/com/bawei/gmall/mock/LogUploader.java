package com.bawei.gmall.mock;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @ClassName LogUploader
 * @Description TODO
 * @Author mufeng_xky
 * @Date 2020/3/6 9:13
 * @Version V1.0
 **/
public class LogUploader {
    //发送日志工具类
    public static void sendLogStream(String log) {
        try {

            //logserver：8080 hadoop路径地址
            URL url = new URL("http://logserver/log");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("clientTime", System.currentTimeMillis() + "");
            conn.setDoOutput(true);
            // http://localhosts/log?log=xxx{json}
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            System.out.print("upload" + log);
            // 输出流
            OutputStream out = conn.getOutputStream();
            out.write(("logString=" + log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
