package com.analyze.util;

import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: zhang yufei
 * @create: 2020-06-15 09:30
 **/
public class HttpUtil {

    public static Map<String, Object> getWebPage(String httpUrl){
        Map<String, Object> resultMap = new HashMap<>();
        try {
            URL url1 = new URL(httpUrl);//使用java.net.URL
            URLConnection connection = url1.openConnection();//打开链接
            InputStream in = connection.getInputStream();//获取输入流
            InputStreamReader isr = new InputStreamReader(in);//流的包装
            BufferedReader br = new BufferedReader(isr);

            String line;
            StringBuffer sb = new StringBuffer();
            while ((line = br.readLine()) != null) {//整行读取
                sb.append(line, 0, line.length());//添加到StringBuffer中
                sb.append('\n');//添加换行符
            }
            String regexBegin="</span>";
            String regexEnd="</p>";
            Pattern proInfo = Pattern.compile(regexBegin+".*?"+regexEnd);
            Matcher matcher = proInfo.matcher(sb);
            if (matcher.find()) {
                System.out.println(matcher.groupCount());
                System.out.println(matcher.group());
            }
//            System.out.println(sb);
            //关闭各种流，先声明的后关闭
            br.close();
            isr.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
        return resultMap;
    }

    public static Map<String, Object> getApi(String httpUrl){
        Map<String, Object> resultMap = new HashMap<>();
        try {
            URL url = new URL(httpUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true); // 设置可输入
            connection.setDoOutput(true); // 设置该连接是可以输出的
            connection.setRequestMethod("GET"); // 设置请求方式
//            connection.setRequestProperty("Content-Type", "application/json;charset=UTF-8");
            OutputStream outputStream = connection.getOutputStream();
            PrintWriter pw = new PrintWriter(outputStream);
//            pw.write(param);
            pw.flush();
            pw.close();
            InputStream inputStream = connection.getInputStream();
            if (connection.getResponseCode() == 200) {
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "gb2312");
                BufferedReader br = new BufferedReader(inputStreamReader);
                String line = null;
                while ((line = br.readLine()) != null) { // 读取数据
//                    System.out.println(line);
                    resultMap.putAll(JSONObject.parseObject(line,Map.class));
                }
            }
            connection.disconnect();
            return resultMap;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultMap;
    }

}
