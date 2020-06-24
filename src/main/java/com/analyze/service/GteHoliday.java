package com.analyze.service;


import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.analyze.util.DatabasePool;
import com.analyze.util.HttpUtil;
import org.apache.spark.sql.sources.In;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: zhang yufei
 * @create: 2020-06-12 16:57
 **/
public class GteHoliday {

    public static void main(String args[]) {
        String path="D:\\0-program\\work\\idea\\analyze_project\\src\\main\\resources\\db.properties";
        if(args.length>0){
            path=args[0];
        }

        Map<String,String> holidayMap=new LinkedHashMap<>();
        List<String> yearLit=new LinkedList<>();

        String begin="2011";
        Calendar dateNow = Calendar.getInstance();
        String end = String.valueOf(dateNow.get(Calendar.YEAR));

        Integer beginInt= Integer.parseInt(begin);
        Integer endInt=Integer.parseInt(end);

        for(Integer i=beginInt;i<=endInt;i++){
            yearLit.add(i.toString());
        }

        for(String year:yearLit){
            getHoliday(year,holidayMap);
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Map<String,String> result=new LinkedHashMap<>();
        for (String key : holidayMap.keySet()) {
            Date inDate = null;
            try {
                inDate = sdf.parse( key );
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if("1".equals(holidayMap.get(key)) && inDate.getDay()!=0 && inDate.getDay()!=6){
                String outDate = sdf.format(inDate);
                result.put(outDate,holidayMap.get(key));
            }else if("2".equals(holidayMap.get(key))){
                String outDate = sdf.format(inDate);
                result.put(outDate,holidayMap.get(key));
            };
        }

        try {
            DatabasePool dbp = DatabasePool.getInstance(path);
            DruidPooledConnection con = dbp.getConnection();
            PreparedStatement ps=null;
            String deleteSql="delete from sys_holiday";
            ps= con.prepareStatement(deleteSql);
            ps.execute();
            for(String key:result.keySet()){
                System.out.println(key+"\t"+result.get(key));
                String sql="insert into sys_holiday values('"+key+"','"+result.get(key)+"')";
                ps= con.prepareStatement(sql);
                ps.execute();
            }
            ps.close();
            con.close();
//            dbp = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void getHoliday(String year, Map<String,String> holidayMap){
        String httpUrl="https://sp0.baidu.com/8aQDcjqpAAV3otqbppnN2DJv/api.php?query="+year+"&resource_id=6018";
        Map<String,Object> result= HttpUtil.getApi(httpUrl);
        JSONArray jsonArray=((JSONArray)result.get("data")).getJSONObject(0).getJSONArray("holiday");
        for(int i=0;i<jsonArray.size();i++){
            JSONObject holiday=jsonArray.getJSONObject(i);
            JSONArray list=holiday.getJSONArray("list");
            for(int j=0;j<list.size();j++){
                JSONObject date=list.getJSONObject(j);
                holidayMap.put(date.getString("date"),date.getString("status"));
            }
        }
    }

}
