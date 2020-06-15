package com.analyze.util;

import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * @author: zhang yufei
 * @create: 2020-06-10 17:55
 **/
public class DateUtil {

    private static StringBuffer holidays=new StringBuffer();
    private static StringBuffer workdays=new StringBuffer();

    static {
        String sql="select * from sys_holiday";
        try {
            DatabasePool dbp = DatabasePool.getInstance();
            DruidPooledConnection con = dbp.getConnection();
            PreparedStatement ps = con.prepareStatement(sql);
            ResultSet result=ps.executeQuery();
            while (result.next()){
                if("1".equals(result.getString("type"))){
                    holidays.append(result.getString("holiday")).append(",");
                }else if("2".equals(result.getString("type"))){
                    workdays.append(result.getString("holiday")).append(",");
                }
            }
            result.close();
            ps.close();
            con.close();
//            dbp = null;
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public DateUtil() {

        String sqlholiday = "select holidaydate from hrmpubholiday t where changetype=1";// 这个表手动维护，由国务院发布放假通知后添加
        String sqlworkday = "select holidaydate from hrmpubholiday t where changetype=3";

//        result = db.executeQuery(sqlholiday);
//        // 得到今年和明年的所有的假日
//        if (resultSet != null && resultSet.getRowCount() != 0) {
//            for (int i = 0; i < result.getRowCount(); i++) {
//                Map row = result.getRows()[i];
//                holidays += row.get("holidaydate").toString() + ",";
//            }
//        }
//        System.out.println(holidays);
//        // 得到今年和明年的所有的假日
//        result = db.executeQuery(sqlworkday);
//        if (result != null && result.getRowCount() != 0) {
//            for (int i = 0; i < result.getRowCount(); i++) {
//                Map row = result.getRows()[i];
//                workdays += row.get("holidaydate").toString() + ",";
//
//            }
//        }
        System.out.println(workdays);
    }

    public static int countWorkDay(String start, String end) {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = df.parse(start);
            endDate = df.parse(end);
        } catch (ParseException e) {
            System.out.println("非法的日期格式,无法进行转换");
            e.printStackTrace();
        }
//        int result = 0;
//        while (startDate.compareTo(endDate) <= 0) {
//            if (startDate.getDay() != 6 && startDate.getDay() != 0)
//                result++;
//            startDate.setDate(startDate.getDate() + 1);
//        }
        int days = getDutyDays(startDate, endDate);
        return days;
    }

    public static int getDutyDays(Date StartDate, Date EndDate) {//得到非周六周日的工作日
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        StartDate.setDate(StartDate.getDate() + 1);
        int result = 0;
        while (StartDate.compareTo(EndDate) <= 0) {
            if (StartDate.getDay() != 6 && StartDate.getDay() != 0) {
                //周内
                result++;
                // System.out.println(holidays);
                if (holidays.length()>0) {
                    if (holidays.indexOf(sdf.format(StartDate)) > -1)//不是节假日加1
                        result--;
                }
            } else {
                //周末
                if (workdays.length()>0) {
                    if (workdays.indexOf(sdf.format(StartDate)) > -1)//工作日
                        result++;
                }
            }
            //  System.out.println(StartDate+"-------"+StartDate.getDay()+"-----"+result+"******"+holidays.indexOf(sdf.format(StartDate))+"******"+workdays.indexOf(sdf.format(StartDate)));
            StartDate.setDate(StartDate.getDate() + 1);
        }
        return result;
    }


}
